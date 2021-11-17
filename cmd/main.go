package main

import (
	"fmt"
	flag "github.com/spf13/pflag"
	"strings"
	"testPulsar/lib/logger"
	"testPulsar/test_case"
)

const brokers = "pulsar://10.109.6.120:6650"
var typeName = flag.StringP("typeName", "t", "p", "input demo type")

func main() {
	logger.Setup(&logger.Settings{
		Path:       "logs",
		Name:       "godis",
		Ext:        "log",
		TimeFormat: "2006-01-02",
	})
	flag.CommandLine.SetNormalizeFunc(wordSepNormalizeFunc)
	flag.Parse()
	switch *typeName {
	case "1":
		test_case.Case1(brokers)
	case "2":
		test_case.Case2(brokers)
	case "3":
		test_case.Case3(brokers)
	default:
		fmt.Println("Please select test case type")
	}
}

func wordSepNormalizeFunc(f *flag.FlagSet, name string) flag.NormalizedName {
	from := []string{"-", "_"}
	to := "."
	for _, sep := range from {
		name = strings.Replace(name, sep, to, -1)
	}
	return flag.NormalizedName(name)
}
