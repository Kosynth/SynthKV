package logger

import (
	"KVwithWAL/config"
	"log"
	"sync"
)

const (
	DEBUG = "DEBUG"
	INFO  = "INFO"
	WARN  = "WARN"
	ERROR = "ERROR"
)

type Logger struct {
	level string
}

var Log *Logger
var once sync.Once

func InitLog() {
	once.Do(func() {
		Log = &Logger{level: config.AppConfig.LoggingLevel}
	})
}

func (l *Logger) Debug(msg string, v ...interface{}) {
	if l.level <= DEBUG {
		log.Printf("[DEBUG] "+msg, v...)
	}
}

func (l *Logger) Info(msg string, v ...interface{}) {
	if l.level <= INFO {
		log.Printf("[INFO] "+msg, v...)
	}
}

func (l *Logger) Warn(msg string, v ...interface{}) {
	if l.level <= WARN {
		log.Printf("[WARN] "+msg, v...)
	}
}

func (l *Logger) Error(msg string, v ...interface{}) {
	if l.level <= ERROR {
		log.Printf("[ERROR] "+msg, v...)
	}
}
