package n3context

import (
	"fmt"
	"os"
	"testing"
)

func TestAddTempContextConfigFromCSV(t *testing.T) {
	type args struct {
		csvName string
		csvStr  string
	}
	tests := []struct {
		name    string
		args    args
		want    string
		wantErr bool
	}{
		// TODO: Add test cases.
		{
			name: "OK",
			args: args{
				csvName: "TempModel1",
				csvStr:  "",
			},
		},
	}

	bytes, err := os.ReadFile("./example/n3ctx/sample_data/maps/levelMaps/scoresMap.csv")
	if err == nil {
		tests[0].args.csvStr = string(bytes)
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cfgtxt, err := AddTempContextConfigFromCSV(tt.args.csvName, tt.args.csvStr)
			fmt.Println(cfgtxt)
			fmt.Println(err)
		})
	}
}
