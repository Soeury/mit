package mlog

import (
	"io"
	"log"
	"os"
	"sync"
)

// 前缀颜色区分： 31m-红   32-绿   34m-蓝
// 日志格式：日期时间 + 短文件名
var (
	errorLog = log.New(os.Stdout, "\033[31m[error]\033[0m", log.LstdFlags|log.Lshortfile)
	infoLog  = log.New(os.Stdout, "\033[34m[infor]\033[0m", log.LstdFlags|log.Lshortfile)
	debugLog = log.New(os.Stdout, "\033[32m[debug]\033[0m", log.LstdFlags|log.Lshortfile)

	loggers = []*log.Logger{errorLog, infoLog, debugLog}
	mu      sync.Mutex
)

var (
	Error  = errorLog.Println
	Errorf = errorLog.Printf
	Info   = infoLog.Println
	Infof  = infoLog.Printf
	Debug  = debugLog.Println
	Debugf = debugLog.Printf
)

const (
	InfoLevel  = iota // 0  显示所有日志
	DebugLevel        // 1  显示 debug 和 error
	ErrorLevel        // 2  仅显示 error 日志
	Disabled          // 3  禁用所有日志
)

func SetLevel(level int) {
	mu.Lock()
	defer mu.Unlock()

	for _, logger := range loggers {
		logger.SetOutput(os.Stdout)
	}

	if ErrorLevel < level {
		errorLog.SetOutput(io.Discard)
	}

	if DebugLevel < level {
		debugLog.SetOutput(io.Discard)
	}

	if InfoLevel < level {
		infoLog.SetOutput(io.Discard)
	}
}
