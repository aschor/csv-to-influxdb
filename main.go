package main

import (
	"encoding/csv"
	"fmt"
	"io"
	"log"
	"os"
	"regexp"
	"strconv"
	"strings"
	"time"
    "reflect"

	"github.com/influxdata/influxdb/client/v2"
	"github.com/jpillora/backoff"
	"github.com/jpillora/opts"
)

var VERSION = "0.0.0-src"

type config struct {
	CSVFile         string `type:"arg" help:"<csv-file> must be a path a to valid CSV file with an initial header row"`
	Server          string `help:"Server address"`
	Database        string `help:"Database name"`
	Measurement     string `help:"Measurement name"`
	BatchSize       int    `help:"Batch insert size"`
	TagColumns      string `help:"Separator(Comma)-separated list of columns to use as tags instead of fields. See Separator option"`
	TimestampColumn string `short:"ts" help:"Header name of the column to use as the timestamp"`
	TimestampFormat string `short:"tf" help:"Timestamp format used to parse all timestamp records"`
	NoAutoCreate    bool   `help:"Disable automatic creation of database"`
    Separator       string `short:"F" help:"input CSV separator character"`
}

func main() {

	//configuration defaults
	conf := config{
		Server:          "http://localhost:8086",
		Database:        "test",
		Measurement:     "data",
		BatchSize:       5000,
		TimestampColumn: "timestamp",
		TimestampFormat: "2006-01-02 15:04:05",
        Separator:       ",",
	}

	//parse config
	opts.New(&conf).
		Name("csv-to-influxdb").
		Repo("github.com/jpillora/csv-to-influxdb").
		Version(VERSION).
		Parse()

    var seprune = []rune(conf.Separator)[0] //string to rune conversion
    
	//set tag names
	tagNames := map[string]bool{}
	for _, name := range strings.Split(conf.TagColumns, conf.Separator) {
		name = strings.TrimSpace(name)
		if name != "" {
			tagNames[name] = true
		}
	}

	//regular expressions
	numbersRe := regexp.MustCompile(`\d`)
	integerRe := regexp.MustCompile(`^\d+$`)
	floatRe := regexp.MustCompile(`^\d+\.\d+$`)
	trueRe := regexp.MustCompile(`^(true|T|True|TRUE)$`)
	falseRe := regexp.MustCompile(`^(false|F|False|FALSE)$`)
	timestampRe, err := regexp.Compile("^" + numbersRe.ReplaceAllString(conf.TimestampFormat, `\d`) + "$")
	if err != nil {
		log.Fatalf("time stamp regexp creation failed")
	}

	//influxdb client
	c,err := client.NewHTTPClient(client.HTTPConfig{
		Addr: conf.Server})
	if err != nil {
                log.Fatalf("Invalid server address: %s", err)
        }

	dbsResp, err := c.Query(client.Query{Command: "SHOW DATABASES"})
	if err != nil {
		log.Fatalf("Invalid server address: %s", err)
	}
	dbExists := false
	for _, v := range dbsResp.Results[0].Series[0].Values {
		dbName := v[0].(string)
		if conf.Database == dbName {
			dbExists = true
			break
		}
	}

	if !dbExists {
		if conf.NoAutoCreate {
			log.Fatalf("Database '%s' does not exist", conf.Database)
		}
		_, err := c.Query(client.Query{Command: "CREATE DATABASE \"" + conf.Database + "\""})
		if err != nil {
			log.Fatalf("Failed to create database: %s", err)
		}
	}

	//open csv file
	f, err := os.Open(conf.CSVFile)
	if err != nil {
		log.Fatalf("Failed to open %s", conf.CSVFile)
	}

	//headers and init fn
	var firstField string
	var headers []string
	setHeaders := func(hdrs []string) {
		//check timestamp and tag columns
		hasTs := false
		n := len(tagNames)
		for _, value := range hdrs {
			if value == conf.TimestampColumn {
				hasTs = true
			} else if tagNames[value] {
				log.Println(value)
				n--
			} else if firstField == "" {
				firstField = value
			}
		}
		if firstField == "" {
			log.Fatalf("You must have at least one field (non-tag)")
		}
		if !hasTs {
			log.Fatalf("Timestamp column (%s) does not match any header (%s)", conf.TimestampColumn, strings.Join(headers, ","))
		}
		if n > 0 {
			log.Fatalf("Tag names (%s) to do not all have matching headers (%s)", conf.TagColumns, strings.Join(headers, ","))
		}
		headers = hdrs
	}

	var bpConfig = client.BatchPointsConfig{Database: conf.Database}
	bp, _ := client.NewBatchPoints(bpConfig) //current batch
	bpSize := 0
	totalSize := 0

	// lastCount := ""

	//write the current batch
 	write := func() {
		if bpSize == 0 {
			return
		}
		b := backoff.Backoff{}
		for {
			if err := c.Write(bp); err != nil {
				d := b.Duration()
				log.Printf("Write failed: %s (retrying in %s)", err, d)
				time.Sleep(d)
				continue
			}
			break
		}
		//TODO(jpillora): wait until the new points become readable
		// count := ""
		// for count == lastCount {
		// 	resp, err := c.Query(client.Query{Command: "SELECT count(" + firstField + ") FROM " + conf.Measurement, Database: conf.Database})
		// 	if err != nil {
		// 		log.Fatal("failed to count rows")
		// 	}
		// 	count = resp.Results[0].Series[0].Values[0][1].(string)
		// }
		//reset
		bp, _ = client.NewBatchPoints(bpConfig)
		bpSize = 0
	} 
    
    //scan des premieres lignes pour determiner le type des mesures
    fieldsKinds := map[string]interface{}{} //association nom de mesure / type
	r := csv.NewReader(f)
    
    r.Comma = seprune // Use user-defined-delimitor instead of comma
    
    nokfields := 1
	for i := 0; i < 100 ; i++ {
        records, err := r.Read()
		if nokfields == 0 {
            break //on a trouve le type de toutes les mesures
        }
        nokfields = 0 //si on trouve tous les champs sur cette ligne ci, on reste à 0 et on sort de la boucle à la prochaine itération
        if err != nil {
			if err == io.EOF {
				break
			}
			log.Fatalf("CSV error: %s", err)
		}
		if i == 0 {
			setHeaders(records)
            nokfields = 1 //pour continer à chercher après la ligne des headers ...
			continue
		}
        
		for key, value := range headers {
			r := records[key]
			//tags are just strings
			if tagNames[value] {
				continue
			}
			//fields require string parsing && on ne veut pas que leur type change au fur et à mesure qu'on rencontre des NULL ou des champs vides, donc on les cherche une bonne fois pour toutes, sur les 100 premieres lignes.
            _,ok := fieldsKinds[value]            
			if !ok {
                nokfields++
                if timestampRe.MatchString(r) {
                    nokfields--
                    continue
                } else if integerRe.MatchString(r) {
                    i, _ := strconv.Atoi(r)
                    fieldsKinds[value] = reflect.TypeOf(i);
                    nokfields--
                } else if floatRe.MatchString(r) {
                    f, _ := strconv.ParseFloat(r, 64)
                    fieldsKinds[value] = reflect.TypeOf(f);
                    nokfields--
                } else if trueRe.MatchString(r) {
                    fieldsKinds[value] = reflect.TypeOf(true);
                    nokfields--
                } else if falseRe.MatchString(r) {
                    fieldsKinds[value] = reflect.TypeOf(false);
                    nokfields--
                } //si null, on verra sur les lignes suivantes. Et une mesure n'est pas sensée être un string. 
            } 
		}        
        
    }
    f.Close()
    
	//open csv file again
	f, err = os.Open(conf.CSVFile)
	if err != nil {
		log.Fatalf("Failed to open %s", conf.CSVFile)
	}    
    
    //////TEST : affichier les types
    for _,value := range headers {
            _,isafield := fieldsKinds[value]
            if tagNames[value] {
				fmt.Printf("tag %s : string\n", value)
			} else if value == conf.TimestampColumn {
                fmt.Printf("timestamp %s\n", value)
            } else if isafield {
                fmt.Printf("mesure %s : %s\n", value, fieldsKinds[value])
            } else {
                fmt.Printf("pas de type trouvé sur les 100 premières lignes pour la colonne : %s\n",value)
                fmt.Printf("sortie ...")
                os.Exit(1)
            }
    }
  
     
	//read csv, line by line
	r = csv.NewReader(f)
    r.Comma = seprune // Use user-defined-delimitor instead of comma
	for i := 0; ; i++ {
		records, err := r.Read()
		if err != nil {
			if err == io.EOF {
				break
			}
			log.Fatalf("CSV error: %s", err)
		}
		if i == 0 {
			setHeaders(records)
			continue
		}

		// Create a point and add to batch
		tags := map[string]string{}
		fields := map[string]interface{}{}

		var ts time.Time

		//move all into tags and fields
		for key, value := range headers {
			r := records[key]
			//tags are just strings
			if tagNames[value] {
				tags[value] = r
				continue
			}
			//fields require string parsing
			if timestampRe.MatchString(r) {
				t, err := time.Parse(conf.TimestampFormat, r)
				if err != nil {
					fmt.Printf("#%d: %s: Invalid time: %s\n", i, value, err)
					continue
				}
				if conf.TimestampColumn == value {
					ts = t //the timestamp column!
					continue
				}
				fields[value] = t
//			} else if integerRe.MatchString(r) {
			} else if fieldsKinds[value] == reflect.TypeOf(2) {
				i, _ := strconv.Atoi(r)
				fields[value] = i
//			} else if floatRe.MatchString(r) {
			} else if fieldsKinds[value] == reflect.TypeOf(2.2) {
				f, _ := strconv.ParseFloat(r, 64)
				fields[value] = f
//			} else if trueRe.MatchString(r) {
			} else if fieldsKinds[value] == reflect.TypeOf(true) {
				fields[value] = true
//			} else if falseRe.MatchString(r) {
			} else if fieldsKinds[value] == reflect.TypeOf(false) {
				fields[value] = false
			} else {
				fields[value] = r //probable crash DB car type de mesure inconnu
			}
		}

		pt, err := client.NewPoint(conf.Measurement, tags, fields, ts)
		bp.AddPoint(pt)

		bpSize++
		totalSize++
		if bpSize == conf.BatchSize {
			write()
		}
	}
	//send remainder
	write()
	log.Printf("Done (wrote %d points)", totalSize) 
}

