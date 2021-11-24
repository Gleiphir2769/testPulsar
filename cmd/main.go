package main

import (
	"fmt"
	flag "github.com/spf13/pflag"
	"strings"
	"testPulsar/lib/logger"
	"testPulsar/test_case"
)

var caseName = flag.Int32P("case", "c", 1, "input case name")
var brokerList = flag.StringP("brokers", "b", "", "input brokers url")
var topic = flag.StringP("topic", "t", "", "input topic name")
var sub = flag.StringP("sub", "s", "", "input subscription name")
var count = flag.Int32P("num", "n", -1, "produce/consume msg nums")

func main() {
	logger.Setup(&logger.Settings{
		Path:       "logs",
		Name:       "testPulsar",
		Ext:        "log",
		TimeFormat: "2006-01-02",
	})
	flag.CommandLine.SetNormalizeFunc(wordSepNormalizeFunc)
	flag.Parse()
	if len(*brokerList) == 0 {
		logger.Error("broker list can not be empty")
	}
	if len(*topic) == 0 {
		logger.Error("topic can not be empty")
	}
	if *caseName != 1 && len(*sub) == 0 {
		logger.Error("sub can not be empty in case 1/3")
	}

	url := fmt.Sprintf("pulsar://%s:6650", *brokerList)

	switch *caseName {
	case 1:
		test_case.Case1(url, *topic, *sub, int(*count))
	case 2:
		test_case.Case2(url, *topic, int(*count))
	case 3:
		test_case.Case3(url, *topic, *sub, int(*count))
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
