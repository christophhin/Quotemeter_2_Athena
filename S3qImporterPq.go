package main

import (
  "fmt"
  "os"
  "time"
  "os/exec"
  "path/filepath"
  "gopkg.in/ini.v1"
  "io/ioutil"
  "encoding/json"
  "bytes"
  "net/http"
  "io"
  "bufio"
  "strings"
  "regexp"
  "strconv"
  "net/url"
  "crypto/tls"

  "github.com/xitongsys/parquet-go-source/local"
  "github.com/xitongsys/parquet-go/parquet"
  "github.com/xitongsys/parquet-go/writer"
)

type iniStruct struct {
  host          string
  port          int
  user          string
  pswd          string
  search        string
  environ       string
  url           string
  username      string
  accountId     string
  roleName      string
  password      string
  region        string
  s3Prefix      string
}


type payloadStruct struct {
  Username  string `json:"username"`
  AccountID string `json:"accountId"`
  RoleName  string `json:"roleName"`
  Password  string `json:"password"`
}

type responseStruct struct {
  AwsAccessKey    string `json:"awsAccessKey"`
  AwsSecretKey    string `json:"awsSecretKey"`
  AwsSessionToken string `json:"awsSessionToken"`
}

type recordStruct struct {
  Symbol            string `parquet:"name=symbol, type=BYTE_ARRAY, convertedtype=UTF8, encoding=PLAIN_DICTIONARY"`
  AccountNumber     string `parquet:"name=accounntnumber, type=BYTE_ARRAY, convertedtype=UTF8, encoding=PLAIN_DICTIONARY"` /* spelling */
  UserID            string `parquet:"name=userid, type=BYTE_ARRAY, convertedtype=UTF8, encoding=PLAIN_DICTIONARY"`
  DepartmentID      string `parquet:"name=departmentid, type=BYTE_ARRAY, convertedtype=UTF8, encoding=PLAIN_DICTIONARY"`
  Exchange          string `parquet:"name=exchange, type=BYTE_ARRAY, convertedtype=UTF8, encoding=PLAIN_DICTIONARY"`
  TotalUsage        int32  `parquet:"name=totalusage, type=INT32, encoding=PLAIN"`
  OptionsChainCount int32  `parquet:"name=optionschaincount, type=INT32, encoding=PLAIN"`
  SubMarket         int32  `parquet:"name=submarket, type=INT32, encoding=PLAIN"`
  ProNonProStatus   string `parquet:"name=prononprostatus, type=BYTE_ARRAY, convertedtype=UTF8, encoding=PLAIN_DICTIONARY"`
  AppID             string `parquet:"name=appid, type=BYTE_ARRAY, convertedtype=UTF8, encoding=PLAIN_DICTIONARY"`
  UsageDate         string `parquet:"name=usagedate, type=BYTE_ARRAY, convertedtype=UTF8, encoding=PLAIN_DICTIONARY"`
  CSPAccountNumber  string `parquet:"name=cspaccountnumber, type=BYTE_ARRAY, convertedtype=UTF8, encoding=PLAIN_DICTIONARY"`
  ExtraValue1       string `parquet:"name=extravalue1, type=BYTE_ARRAY, convertedtype=UTF8, encoding=PLAIN_DICTIONARY"`
  ExtraValue2       string `parquet:"name=extravalue2, type=BYTE_ARRAY, convertedtype=UTF8, encoding=PLAIN_DICTIONARY"`
}

func readINI() iniStruct {
  // --- find ini file ---
  file, _ := os.Readlink("/proc/self/exe")

  // --- read ini file ---
  cfg, err := ini.Load(filepath.Join(filepath.Dir(file), "S3qImporterPq.ini"))
  if err != nil {
    panic(err.Error())
  }

  port, err := cfg.Section("splunk").Key("port").Int() 

  ini := iniStruct {
    cfg.Section("splunk").Key("host").String(),
    port,
    cfg.Section("splunk").Key("user").String(),
    cfg.Section("splunk").Key("pswd").String(),
    cfg.Section("splunk").Key("search").String(),
    cfg.Section("splunk").Key("environ").String(),
    cfg.Section("main").Key("url").String(),
    cfg.Section("main").Key("username").String(),
    cfg.Section("main").Key("accountId").String(),
    cfg.Section("main").Key("roleName").String(),
    cfg.Section("main").Key("password").String(),
    cfg.Section("main").Key("region").String(),
    cfg.Section("main").Key("s3Prefix").String(),
  }
  return ini
}

func convertDT(dt string) string {
  a := strings.Split(dt, " ")
  d := strings.Split(a[0], "/")
  
  return fmt.Sprintf("%s-%s-%s %s", d[2], d[0], d[1], a[1])
}

func date2epoch(dt string) int64 {
  _, offset     := time.Now().Local().Zone()

  t, err := time.Parse("2006-01-02", dt)
  if err != nil {
    panic("Wrong date!")
  }
  
  return t.Unix() - int64(offset)
}

func splunkQuery(ini iniStruct, dtStr string) {
  outFile := fmt.Sprintf("/tmp/dailyQuote_%s.csv", dtStr)
  sTime := date2epoch(dtStr)
  eTime := sTime + 86400
  search   := url.QueryEscape(fmt.Sprintf(ini.search, ini.environ))

  tr := &http.Transport {
	  TLSClientConfig: &tls.Config{InsecureSkipVerify: true},
  }
  client := &http.Client{Transport: tr}
  
  bodyStr := fmt.Sprintf("search=search %s&earliest_time=%d&latest_time=%d&output_mode=csv", search, sTime, eTime)
  body := strings.NewReader(bodyStr)
  url  := "https://" + ini.host + ":" + fmt.Sprintf("%d", ini.port) + "/servicesNS/admin/search/search/jobs/export"

  req, err := http.NewRequest("POST", url, body)
  if err != nil {
    panic(err.Error())
  }
  req.SetBasicAuth(ini.user, ini.pswd)
  req.Header.Set("Content-Type", "application/x-www-form-urlencoded")

  fmt.Println("----- [ Splunk REST request started. ] -----")

  resp, err := client.Do(req)
  if err != nil {
    panic(err.Error())
  }
  defer resp.Body.Close()  

  f, err := os.Create(outFile)
  if err != nil {
    panic(err.Error())
  }
  defer f.Close()

  _, err = io.Copy(f, resp.Body)
  if err != nil {
    panic(err.Error())
  }

  fmt.Println("----- [ Splunk REST request finished. ] -----")
}

func getS3creds(ini iniStruct) {
  var response responseStruct

  data := payloadStruct {
    ini.username,
    ini.accountId,
    ini.roleName,
    ini.password,
  }
  
  payloadBytes, err := json.Marshal(data)
  if err != nil {
    panic(err.Error())
  }
  body := bytes.NewReader(payloadBytes)
  req, err := http.NewRequest("POST", ini.url, body)
  if err != nil {
    panic(err.Error())
  }
  req.Header.Set("Content-Type", "application/json")
  resp, err := http.DefaultClient.Do(req)
  if err != nil {
    panic(err.Error())
  }
  defer resp.Body.Close()    

  bodyBytes, err := ioutil.ReadAll(resp.Body)
  
  err = json.Unmarshal(bodyBytes, &response)
  if err != nil {
    panic(err.Error())
  }

  // --- create aws cli config file ---
  config := "/root/.aws/credentials"
  f, err := os.Create(config)
  if err != nil {
    panic(err.Error())
  }

  fmt.Fprintln(f, "[default]")
  fmt.Fprintln(f, fmt.Sprintf("aws_access_key_id = %s", response.AwsAccessKey))
  fmt.Fprintln(f, fmt.Sprintf("aws_secret_access_key = %s", response.AwsSecretKey))
  fmt.Fprintln(f, fmt.Sprintf("aws_session_token = %s", response.AwsSessionToken))
  f.Close()
  err = os.Chmod(config, 0600)
  if err != nil {
    panic(err.Error())
  }
}

func checkFile(f string) bool {
  _, err := os.Stat(f)
  if err != nil {
    return false
  } 
  
  return true
}

func copyFile(ini iniStruct, dtStr string) {
  var cspCheck = regexp.MustCompile(`^([0-9]){5,7}|mdx`)
  aMap := make(map[string]int)

  dtArr := strings.Split(dtStr, "-")
  // --- source file ---
  srcFile := fmt.Sprintf("/tmp/dailyQuote_%s.csv", dtStr)
  if ! checkFile(srcFile) {
    fmt.Printf("%s doesn't exist\n", srcFile)
    os.Exit(1)
  }

  inFile, err := os.Open(srcFile)
  if err != nil {
    panic(err.Error())
  }
  defer inFile.Close()
  
  scanner := bufio.NewScanner(inFile)

  // skip 1st line
  scanner.Scan()

  // --- create customer list ---
  for scanner.Scan() {
    record := strings.Split(strings.Trim(scanner.Text(), "\""), ";")

    if cspCheck.MatchString(record[11]) {
      aMap[record[11]]++
    }
  }
  
  // --- loop customer list and create dedicated Parquet files ---
  for k := range aMap {
    fmt.Printf("%v\n", k)
    
    // --- rewind input file ---
    inFile.Seek(0, 0)
    
    // --- open output file ---
    outFileName := fmt.Sprintf("/tmp/dailyQuote_%s.pq", k)
  	fw, err := local.NewLocalFileWriter(outFileName)
	  if err != nil {
      panic(err.Error())
	  }

    pw, err := writer.NewParquetWriter(fw, new(recordStruct), 4)
    if err != nil {
      panic(err.Error())
    }

    //pw.RowGroupSize = 512 * 1024 * 1024 //128M
    //pw.PageSize = 8 * 1024
    pw.CompressionType = parquet.CompressionCodec_GZIP

    // --- read input, search and write output ---   
    scanner := bufio.NewScanner(inFile)

    // skip 1st line
    scanner.Scan()

    for scanner.Scan() {
      line := strings.Split(strings.Trim(scanner.Text(), "\""), ";")

      if line[11] != k {
        continue
      }
      
      a1, _ := strconv.Atoi(line[5])
      a2, _ := strconv.Atoi(line[6])
      a3, _ := strconv.Atoi(line[9])
      a4    := convertDT(line[1])
      a5    := ""
      a6    := ""
      if len(line) >= 13 {
        a5 = line[12]
      }

      if len(line) == 14 {
        a6 = line[13]
      }

      rec := recordStruct {
        Symbol:            line[10],
        AccountNumber:     line[11],
        UserID:            line[3],
        DepartmentID:      line[2],
        Exchange:          line[4],
        TotalUsage:        int32(a1),
        OptionsChainCount: int32(a2),
        SubMarket:         int32(a3),
        ProNonProStatus:   line[7],
        AppID:             line[8],
        UsageDate:         a4,
        CSPAccountNumber:  line[11],
        ExtraValue1:       a5,
        ExtraValue2:       a6,
      }
      if err = pw.Write(rec); err != nil {
        panic(err.Error())
      }
    }

    // ---flush buffers, close file and upload to S3 bucket ---
  	if err = pw.WriteStop(); err != nil {
        panic(err.Error())
    }
    
    s3Target := fmt.Sprintf("%s/client=%s/year=%s/month=%s/day=%s/dailyQuote", ini.s3Prefix, k, dtArr[0], dtArr[1], dtArr[2])

    // --- execute copy to s3 storage ---
    args := []string{"s3", "cp", outFileName, s3Target}
    out, err := exec.Command("/usr/local/bin/aws", args...).Output()
    if err != nil {
      panic(err.Error())
    }
  
    fmt.Printf("Copy executed: %s\n", string(out))

    // --- delete local client source file ---
    err = os.Remove(outFileName)
    if err != nil {
      panic(err.Error())
    }
  }

  // --- delete local source file ---
  err = os.Remove(srcFile)
  if err != nil {
    panic(err.Error())
  }
}

func main() {
  // --- check command line arguments ---
  if len(os.Args) != 2 {
    fmt.Println("Usaage:")
    fmt.Printf("   %s date\n", os.Args[0])
    os.Exit(-1)
  }
  fmt.Printf("Date: %s\n", os.Args[1])
  
  ini := readINI()

  getS3creds(ini)
  splunkQuery(ini, os.Args[1])
  copyFile(ini, os.Args[1])
  
  os.Exit(0)
}
