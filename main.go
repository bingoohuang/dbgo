package main

import (
	"bytes"
	"encoding/json"
	"flag"
	"fmt"
	"github.com/rogozhka/query"
	"log"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"time"

	// 支持通过设置环境变量 DAEMON=1 后台启动
	_ "github.com/bingoohuang/godaemon/autoload"
	"github.com/xo/dburl"
	"gopkg.in/yaml.v3"
)

func main() {
	confFile := flag.String("c", "./db.yml", "配置文件路径")
	flag.Parse()

	conf, err := parseConf(*confFile)
	if err != nil {
		log.Fatalf("parse conf: %v", err)
	}

	for _, action := range conf.Actions {
		go action.Run()
	}

	select {}
}

type Conf struct {
	Actions []Action `yaml:"actions"`
}

type Action struct {
	DBUrl    string        `yaml:"dbURL"`
	Query    string        `yaml:"query"`
	Duration time.Duration `yaml:"duration"`
	Notify   string        `yaml:"notify"`
}

func (a Action) Run() {
	ticker := time.NewTicker(a.Duration)
	defer ticker.Stop()

	for {
		a.tick()
		<-ticker.C
	}
}

func (a Action) tick() {
	db, err := dburl.Open(a.DBUrl)
	if err != nil {
		log.Fatalf("open %s: %v", a.DBUrl, err)
	}
	defer db.Close()

	// fetch all the rows into slice of map[string]string
	rows, err := query.Fetch(a.Query, db)
	if err != nil {
		panic(err)
	}

	for _, row := range rows {
		json, _ := json.Marshal(row)
		http.Post(a.Notify, "application/json", bytes.NewBuffer(json))
	}
}

func parseConf(filePath string) (*Conf, error) {
	cf, err := os.ReadFile(filePath)
	if err != nil {
		return nil, fmt.Errorf("reading config file %s failed: %w", filePath, err)
	}

	var c Conf

	switch ext := strings.ToLower(filepath.Ext(filePath)); ext {
	case ".yaml", ".yml":
		if err := yaml.Unmarshal(cf, &c); err != nil {
			return nil, fmt.Errorf("unmarshal yaml file %s failed: %w", filePath, err)
		}
		return &c, nil
	case ".json", ".js":
		if err := json.Unmarshal(cf, &c); err != nil {
			return nil, fmt.Errorf("unmarshal json file %s failed: %w", filePath, err)
		}
		return &c, nil
	}

	return nil, fmt.Errorf("unknown file format: %s", filePath)
}
