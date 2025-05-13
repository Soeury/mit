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
	printLog = log.New(os.Stdout, "\033[35m[print]\033[0m", log.LstdFlags|log.Lshortfile)
	solidLog = log.New(os.Stdout, "\033[36m[solid]\033[0m", log.LstdFlags|log.Lshortfile)

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
	Print  = printLog.Println
	Printf = printLog.Printf
	Solid  = solidLog.Println
	Solidf = solidLog.Printf
)

const (
	InfoLevel  = iota // 0  显示所有日志
	PrintLevel        // 1  显示 print debug solid 和 error
	DebugLevel        // 1  显示 debug solid 和 error
	SolidLevel        // 2  显示 solid 和 error
	ErrorLevel        // 3  仅显示 error 日志
	Disabled          // 4  禁用所有日志
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

	if SolidLevel < level {
		solidLog.SetOutput(io.Discard)
	}

	if DebugLevel < level {
		debugLog.SetOutput(io.Discard)
	}

	if PrintLevel < level {
		printLog.SetOutput(io.Discard)
	}

	if InfoLevel < level {
		infoLog.SetOutput(io.Discard)
	}
}
