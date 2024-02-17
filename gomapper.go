package main

/*

redis data structure

dev_list - HASH IP [pause|run]
ip_lock.Q.IP - redmutex key
ip_last_result.Q.IP - ok:timestart:timedone | error:timestart:timeerror:error msg
ip_data.Q.IP - queue Q HASH  someKey value otherKey.index value ...
ip_keys.Q.IP - queue Q HASH  someKey timestart_ms:timestop_ms:(one|table) otherKey timestart_ms:timestop_ms:(one|table) ...
ip_queues.IP - HASH Q time:nextrun:status:descr ...

ip_oids.Q.IP - queue Q HASH  someKey oid:itemType:valueType:opt:optv ...


*/

import (
  snmp "github.com/soniah/gosnmp"
  "sync"
  "fmt"
  "os"
  "log"
  "time"
  _ "bufio"
  "syscall"
  "os/signal"
  "regexp"
  "net"
  "errors"
  "strings"
  "strconv"
  _ "runtime"
  "github.com/gomodule/redigo/redis"
  "github.com/ShyLionTjmn/redmutex"
  . "github.com/ShyLionTjmn/mapaux"
  "github.com/marcsauter/single"
)

const DEFAULT_SNMP_TIMEOUT=10
const DEFAULT_SNMP_RETRIES=3
const DEFAULT_SNMP_MAX_REPETITIONS=10

const DB_REFRESH_TIME= 10
const DB_ERROR_TIME= 5

const ERROR_SLEEP=15
const IDLE_SLEEP=600

const OIDS_FILE="/etc/gomapper/oids"

const REDIS_SOCKET="/tmp/redis.sock"
const REDIS_DB="0"

var red_db string=REDIS_DB

const IP_REGEX=`^(?:(?:25[0-5]|2[0-4][0-9]|[01]?[0-9][0-9]?)\.){3}(?:25[0-5]|2[0-4][0-9]|[01]?[0-9][0-9]?)$`
const DB_FILE_RECORD="^(\\d{1,3}\\.\\d{1,3}\\.\\d{1,3}\\.\\d{1,3})(?:\\s.*)?$"
var db_file_record_reg *regexp.Regexp
var ip_reg *regexp.Regexp

var errInterrupted = errors.New("Interrupted")

//type M map[string]interface{}

var globalMutex = &sync.Mutex{}
var good_redis int64
var bad_redis int64

var good_oids int64
var bad_oids int64

type t_workStruct struct {
  queue		int
  dev_ip	string
  control_ch	chan string
  data_ch	chan t_scanData
  wg		*sync.WaitGroup
  added		time.Time
  check		time.Time
  conn		net.Conn
  job		[]t_scanJobGroup
}

const (
  //item type
  itOne		=iota
  itTable
  //item value type
  vtInt
  vtUns
  vtString
  vtHex
  vtOid
  //group match type
  mtAny
  mtPrefix
  mtRegex
  mtExact
  mtNone
  //scan data type
  dtExit //goroutine desided to exit
)

var const2str = map[int]string{
  itOne:	"one",
  itTable:	"table",
  //item value type
  vtInt:	"int",
  vtUns:	"uns",
  vtString:	"str",
  vtHex:	"hex",
  vtOid:	"oid",
  //group match type
  mtAny:	"any",
  mtPrefix:	"prefix",
  mtExact:	"exact",
  mtRegex:	"regex",
  mtNone:	"none",
  //scan data type
  dtExit:	"exit",
}

const (
  //item options
  ioMac=1 << iota
  ioArp=1 << iota
  ioFail=1 << iota
  ioIfNot=1 << iota
  ioAuto=1 << iota
  ioMul=1 << iota
)

var optionArg = map[int]bool{
  ioFail:	false,
  ioIfNot:	true,
  ioArp:	true,
  ioMac:	true,
  ioAuto:	false,
  ioMul:	true,
}

var const2option = map[int]string {
  ioFail:	"fail",
  ioIfNot:	"ifnot",
  ioArp:	"arp",
  ioMac:	"mac",
  ioAuto:	"auto",
  ioMul:	"mul",
}

var option2const = make(map[string]int)

func init() {
  for i, v := range const2option { option2const[v] = i }
}

type t_scanData struct {
  ws		*t_workStruct
  data_type	int
  data_str	string
}

type t_scanJobItem struct {
  Line		int
  Item_type	int
  Value_type	int
  Oid		string
  Key		string
  Options	int
  Opt_values	map[int]string
  Value		interface{}
  Item_start	int64
  Item_stop	int64
}

type t_scanJobGroup struct {
  Line		int
  Refresh	int
  Timeout	int64
  Retries	int
  MaxRepetitions uint8
  Last_run	time.Time
  //Last_success	time.Time
  Match_type	int
  Match_str	string
  Unmatch_type	int
  Unmatch_str	string
  Matched	bool
  Items		[]t_scanJobItem
}

func control_in(control_ch chan string) string {
  select {
    case ret, ok := <-control_ch:
      if ok {
        return ret
      } else {
        return "stop"
      }
    default:
      return ""
  }
}

func match_sOID(sOID string, match_str string, match_type int) bool {
  switch match_type {
  case mtAny:
    return true
  case mtNone:
    return false
  case mtPrefix:
    return strings.Index(sOID, match_str) == 0
  case mtRegex:
    reg, err := regexp.Compile(match_str)
    if err != nil {
      return false
    } else {
      return reg.MatchString(sOID)
    }
  case mtExact:
    return sOID == match_str
  default:
    return false
  }
}

func debugPub(red redis.Conn, ws *t_workStruct, debug string, key string, message string) {
  if red == nil || red.Err() != nil { return }
  if debug == "" { return }
  if key != "" && strings.Index(debug, key) >= 0 {
    //if
    red.Do("PUBLISH", "debug", fmt.Sprint(time.Now().Format("2006.01.02 15:04:05.000 "), ws.dev_ip, " ", ws.queue, " ", key, " ", message))
    red.Do("PUBLISH", "debug_gomapper", fmt.Sprint(time.Now().Format("2006.01.02 15:04:05.000 "), ws.dev_ip, " ", ws.queue, " ", key, " ", message))
  }
}

func worker(ws *t_workStruct) {
//  defer func() {
//    r := recover();
//    if r != nil { fmt.Println("worker panicked:", r) }
//  }()
  //defer fmt.Println("worker return")
  defer func() {
    ws.wg.Done()
  }()

  var err error
  var red redis.Conn

  defer func() {
    if red != nil {
      red.Close()
      red = nil
    }
  } ()

  lock_key := fmt.Sprintf("ip_lock.%s", ws.dev_ip)
  last_result_key := fmt.Sprintf("ip_last_result.%d.%s", ws.queue, ws.dev_ip)
  data_key := fmt.Sprintf("ip_data.%d.%s", ws.queue, ws.dev_ip)
  keys_key := fmt.Sprintf("ip_keys.%d.%s", ws.queue, ws.dev_ip)
  oids_key := fmt.Sprintf("ip_oids.%d.%s", ws.queue, ws.dev_ip)
  queues_key := fmt.Sprintf("ip_queues.%s", ws.dev_ip)
  graph_key := fmt.Sprintf("ip_graphs.%s", ws.dev_ip)

  redm := redmutex.New(lock_key)

  //fmt.Println("worker start")

  client := &snmp.GoSNMP{
    Target:    ws.dev_ip,
    Port:      uint16(161),
    Community: "public",
    Version:   snmp.Version2c,
    Timeout:   time.Duration(DEFAULT_SNMP_TIMEOUT) * time.Second,
    Retries:   DEFAULT_SNMP_RETRIES,
    MaxRepetitions: DEFAULT_SNMP_MAX_REPETITIONS,
//    Logger:    log.New(os.Stdout, "", 0),
  }

  err = client.Connect()
  if err != nil {
    log.Fatalf("Connect() err: %v", err)
  }

  ws.conn = client.Conn

  var sysObjectID string

  var val string

  var queue_keys map[string]bool

  var graph_keys map[string]string
  var graph_keys_time int64

  var debug string

  var sysUpTime string

WORKER_CYCLE:
  for {

//fmt.Println("cycle start", ws.dev_ip, ws.queue)

    work_start := time.Now()

    first_poke_ok := false

    if red != nil && red.Err() == nil {
      _, err = red.Do("SELECT", red_db)
      if err != nil {
        red.Close()
        red = nil
      } else {
        first_poke_ok = true
      }
    }

    if red != nil && red.Err() != nil {
      red.Close()
      red=nil
    }

    err = nil

    if red == nil {
      red, err = redis.Dial("unix", REDIS_SOCKET)
      first_poke_ok = false
    }

    if err == nil && red != nil && !first_poke_ok {
      //poke redis to check connectivity
      _, err = red.Do("SELECT", red_db)
      if err != nil {
        red.Close()
        red = nil
      }
    }

    globalMutex.Lock()

    if err == nil && red != nil {
      if good_redis < bad_redis {
        good_redis = time.Now().Unix()
        fmt.Fprintln(os.Stderr, "redis is back")
      }
    } else {
      if bad_redis <= good_redis {
        bad_redis = time.Now().Unix()
        fmt.Fprintln(os.Stderr, "redis is down")
      }
    }
    globalMutex.Unlock()

    if err == nil && red != nil && red.Err() == nil {
      debug, err = redis.String(red.Do("GET", "ip_debug."+ws.dev_ip))
      if err == redis.ErrNil {
        err = nil
      }
    }

    if err == nil && red != nil {
      queue_report := fmt.Sprintf("%d:%d:run:getSOID", work_start.Unix(), work_start.Unix())
      _, err = red.Do("HSET", queues_key, fmt.Sprint(ws.queue), queue_report)
    }

    sOIDstart_time := time.Now()
    sOIDstart := sOIDstart_time.Unix()*1000+int64(sOIDstart_time.Nanosecond()/1000000)

    var sOIDstop_time time.Time
    var sOIDstop int64

    if err == nil && red != nil {
      // get sysObjectID.0
//fmt.Println("getting sOID", ws.dev_ip, ws.queue)
      debugPub(red, ws, debug, "sysObjectID", "get")
      val, err = getOne(client, ".1.3.6.1.2.1.1.2.0", vtOid)
      debugPub(red, ws, debug, "sysObjectID", fmt.Sprint("res: ", val, " ", err))

      sOIDstop_time = time.Now()
      sOIDstop = sOIDstop_time.Unix()*1000+int64(sOIDstop_time.Nanosecond()/1000000)
      if err != nil && len(ws.control_ch) != 0 {
        //got some control command, stop doing job
        err = errInterrupted
      }
    }

    if err == nil && strings.Index(val, ".1.3.6.1.4.1.") != 0 {
      err = errors.New("Bad sysObjectID: "+val)
      debugPub(red, ws, debug, "sysObjectID", "ERROR: bad sysObjectID")
    }

    if err == nil && sysObjectID != val && red != nil {
      debugPub(red, ws, debug, "sysObjectID", "new!")
      sysObjectID = val

      queue_keys = make(map[string]bool)

      var keys_count int
      var ip_oids = redis.Args{}.Add(oids_key)
      for jgi := 0; jgi < len(ws.job); jgi++ {
        ws.job[jgi].Last_run = time.Time{}
        match := match_sOID(sysObjectID, ws.job[jgi].Match_str, ws.job[jgi].Match_type)
        unmatch := !match_sOID(sysObjectID, ws.job[jgi].Unmatch_str, ws.job[jgi].Unmatch_type)
        ws.job[jgi].Matched = match && unmatch
        debugPub(red, ws, debug, "oids", fmt.Sprint("match_sOID: ", sysObjectID, " ", ws.job[jgi].Match_str, " ", ws.job[jgi].Match_type, " ", "res:", " ", match))
        debugPub(red, ws, debug, "oids", "&&")
        debugPub(red, ws, debug, "oids", fmt.Sprint("!match_sOID:", " ", sysObjectID, " ", sysObjectID, " ", ws.job[jgi].Unmatch_str, " ", ws.job[jgi].Unmatch_type, " ", "res:", " ", unmatch))
        debugPub(red, ws, debug, "oids", fmt.Sprint("res:", " ", ws.job[jgi].Matched))
//fmt.Println("match sOID", sysObjectID, "result:", ws.job[jgi].Matched, ws.dev_ip, ws.queue)
        if ws.job[jgi].Matched {
          for ii := 0; ii < len(ws.job[jgi].Items); ii++ {
            ws.job[jgi].Items[ii].Value = nil
            ws.job[jgi].Items[ii].Item_start = 0
            ws.job[jgi].Items[ii].Item_stop = 0
            ip_oids = ip_oids.Add(ws.job[jgi].Items[ii].Key)
            keys_count++
            oids_val := ws.job[jgi].Items[ii].Oid
            oids_val += ":"+fmt.Sprint(ws.job[jgi].Refresh)
            oids_val += ":"+const2str[ ws.job[jgi].Items[ii].Item_type ]
            oids_val += ":"+const2str[ ws.job[jgi].Items[ii].Value_type ]
            opts := ""
            for opt_const, has_args := range optionArg {
              if (ws.job[jgi].Items[ii].Options & opt_const) != 0 {
                if opts != "" { opts += "," }
                opts += const2option[opt_const]
                if has_args {
                  opts += " "+ws.job[jgi].Items[ii].Opt_values[opt_const]
                }
              }
            }
            oids_val += ":"+opts
            ip_oids = ip_oids.Add(oids_val)
          }
        } else {
          for ii := 0; ii < len(ws.job[jgi].Items); ii++ {
            ws.job[jgi].Items[ii].Value = nil
            ws.job[jgi].Items[ii].Item_start = 0
            ws.job[jgi].Items[ii].Item_stop = 0
          }
        }
      }
      ip_oids = ip_oids.Add("_count", keys_count)
      ip_oids = ip_oids.Add("_sysObjectID", sysObjectID)
      red.Send("MULTI")
      red.Send("DEL", oids_key)
      red.Send("HSET", ip_oids...)
      red.Do("EXEC")
    }

    if err == nil && red != nil {
      var redstr string
      redstr, err = redis.String(red.Do("HGET", graph_key, "time"))
      if err == nil {
        if i, _err := strconv.ParseInt(redstr, 10, 64); _err == nil && i > graph_keys_time {
          graph_keys, err = redis.StringMap(red.Do("HGETALL", graph_key))
          if err != nil {
            graph_keys_time = 0
          } else {
            graph_keys_time = i
          }
        }
      }
    }

    if err == redis.ErrNil { err = nil }

    last_report_time := time.Now()
    if err == nil && red != nil {
      report_time := time.Now().Unix()
      queue_report := fmt.Sprintf("%d:%d:run:get data", report_time, report_time)
      _, err = red.Do("HSET", queues_key, fmt.Sprint(ws.queue), queue_report)
    }

    if err == nil && red != nil {
      //get sysUpTime.0 for graph
      sysUpTime, err = getOne(client, ".1.3.6.1.2.1.1.3.0", vtUns)
    }

    if err == nil && red != nil {
JG:   for jgi := 0; jgi < len(ws.job); jgi++ {
//fmt.Println("matched:", ws.job[jgi].Matched, ws.dev_ip, ws.queue)
        if !ws.job[jgi].Matched { continue }
        jg_next_run := ws.job[jgi].Last_run.Add(time.Duration(ws.job[jgi].Refresh)*time.Second)
        debugPub(red, ws, debug, fmt.Sprintf("jg@%d", ws.job[jgi].Line), fmt.Sprint("next_run:", jg_next_run))
        debugPub(red, ws, debug, fmt.Sprintf("jg@%d", ws.job[jgi].Line), fmt.Sprint("VS work_start", " ", work_start))
        if  jg_next_run.Before(work_start) || jg_next_run.Equal(work_start) {
          ws.job[jgi].Last_run=work_start

          debugPub(red, ws, debug, fmt.Sprintf("jg@%d", ws.job[jgi].Line), "run")
          var key_value interface{}

ITEM:     for ii := 0; ii < len(ws.job[jgi].Items); ii++ {
            debugPub(red, ws, debug, ws.job[jgi].Items[ii].Key, "begin")
            // check if key has ioIfNot option and what that oid is unsupported
            if (ws.job[jgi].Items[ii].Options & ioIfNot) != 0 {
              _, key_exists := queue_keys[ ws.job[jgi].Items[ii].Opt_values[ioIfNot] ]
              debugPub(red, ws, debug, ws.job[jgi].Items[ii].Key, fmt.Sprint("skip by ifNot", " ", key_exists))
              if key_exists { continue ITEM }
            }
            item_start := time.Now()
            if last_report_time.Add(time.Second).Before(item_start) {
              last_report_time = item_start
              report_time := item_start.Unix()
              var queue_report string
              var key_info = fmt.Sprintf("key: %s, oid: %s, item: %d", ws.job[jgi].Items[ii].Key, ws.job[jgi].Items[ii].Oid, ws.job[jgi].Items[ii].Line)
              switch ws.job[jgi].Items[ii].Item_type {
              case itOne:
                queue_report = fmt.Sprintf("%d:%d:run:get data, %s", report_time, report_time, key_info)
              case itTable:
                queue_report = fmt.Sprintf("%d:%d:run:get table data, %s", report_time, report_time, key_info)
              }
              red.Do("HSET", queues_key, fmt.Sprint(ws.queue), queue_report)
            }

            client.Timeout = time.Duration(ws.job[jgi].Timeout) * time.Second
            client.Retries = ws.job[jgi].Retries
            client.MaxRepetitions = ws.job[jgi].MaxRepetitions

            switch ws.job[jgi].Items[ii].Item_type {
            case itOne:
              key_value, err = getOne(client, ws.job[jgi].Items[ii].Oid, ws.job[jgi].Items[ii].Value_type)

            case itTable:
              var key_info = fmt.Sprintf("key: %s, oid: %s, item: %d", ws.job[jgi].Items[ii].Key, ws.job[jgi].Items[ii].Oid, ws.job[jgi].Items[ii].Line)
              key_value, err = getTableFunc(client, ws.job[jgi].Items[ii].Oid, ws.job[jgi].Items[ii].Value_type, func() {
                debugPub(red, ws, debug, ws.job[jgi].Items[ii].Key, "beat")
                if last_report_time.Add(time.Second).Before(time.Now()) {
                  last_report_time = time.Now()
                  report_time := last_report_time.Unix()
                  queue_report := fmt.Sprintf("%d:%d:run:get table data, %s", report_time, report_time, key_info)
                  red.Do("HSET", queues_key, fmt.Sprint(ws.queue), queue_report)
                }
              })
            }
            debugPub(red, ws, debug, ws.job[jgi].Items[ii].Key, fmt.Sprint("get err:", " ", err))
            if err != nil {
              if len(ws.control_ch) != 0 {
                err = errInterrupted
                debugPub(red, ws, debug, ws.job[jgi].Items[ii].Key, "interrupted")
              } else {
                if err.Error() == "NoSuchInstance" && (ws.job[jgi].Items[ii].Options & ioFail) == 0 {
//fmt.Println("NoSuchInstance", ws.job[jgi].Items[ii].Oid, ws.dev_ip, ws.queue)
                  //oid not supported, ignore
                  err = nil
                  ws.job[jgi].Items[ii].Value = nil
                  debugPub(red, ws, debug, ws.job[jgi].Items[ii].Key, "ignore error")
                  continue ITEM
                }
                prev_err_text := err.Error()
                err = errors.New(prev_err_text+", key: "+ws.job[jgi].Items[ii].Key+", oid: "+ws.job[jgi].Items[ii].Oid+", item: "+strconv.Itoa(ws.job[jgi].Items[ii].Line))
              //something bad happened
//fmt.Println(err.Error(), ws.job[jgi].Items[ii].Oid, ws.dev_ip, ws.queue)
              }
              debugPub(red, ws, debug, ws.job[jgi].Items[ii].Key, "break JG")
              break JG
            }
            if (ws.job[jgi].Items[ii].Options & ioMul) != 0 {
//fmt.Println("ioMul", ws.job[jgi].Items[ii].Opt_values[ioMul])
              if multiplier, _err := strconv.ParseInt(ws.job[jgi].Items[ii].Opt_values[ioMul], 10, 64); _err == nil {
//fmt.Println(multiplier)
                switch ws.job[jgi].Items[ii].Item_type {
                case itOne:
                  switch ws.job[jgi].Items[ii].Value_type {
                  case vtInt, vtString:
                    if v, _err := strconv.ParseInt(key_value.(string), 10, 64); _err == nil {
                      key_value = strconv.FormatInt( v * multiplier, 10 )
//fmt.Println(key_value)
                    }
                  case vtUns:
                    if v, _err := strconv.ParseUint(key_value.(string), 10, 64); _err == nil && multiplier >= 0 {
                      key_value = strconv.FormatUint( v * uint64(multiplier), 10 )
                    }
                  }
                case itTable:
                  for index, value := range key_value.(map[string]string) {
                    switch ws.job[jgi].Items[ii].Value_type {
                    case vtInt, vtString:
                      if v, _err := strconv.ParseInt(value, 10, 64); _err == nil {
                        key_value.(map[string]string)[index] = strconv.FormatInt( v * multiplier, 10 )
                      }
                    case vtUns:
                      if v, _err := strconv.ParseUint(value, 10, 64); _err == nil && multiplier >= 0 {
                        key_value.(map[string]string)[index] = strconv.FormatUint( v * uint64(multiplier), 10 )
                      }
                    }
                  }
                }
              }
            }
            if graph_keys_time > 0 {
              switch ws.job[jgi].Items[ii].Item_type {
              case itOne:
                if _, ok := graph_keys[ ws.job[jgi].Items[ii].Key+".0" ]; ok {
                  if red != nil && red.Err() == nil {
                    red.Do("PUBLISH", "graph", ws.dev_ip+" "+sysUpTime+" "+ws.job[jgi].Items[ii].Key+".0 "+fmt.Sprint(key_value))
                  }
                }
              case itTable:
                for index, value := range key_value.(map[string]string) {
                  if _, ok := graph_keys[ ws.job[jgi].Items[ii].Key+"."+index ]; ok {
                    if red != nil && red.Err() == nil {
                      red.Do("PUBLISH", "graph", ws.dev_ip+" "+sysUpTime+" "+ws.job[jgi].Items[ii].Key+"."+index+" "+fmt.Sprint(value))
                    }
                  }
                }
              }
            }
            now := time.Now()
            ws.job[jgi].Items[ii].Value = key_value
            ws.job[jgi].Items[ii].Item_start = item_start.Unix()*1000+int64(item_start.Nanosecond()/1000000)
            ws.job[jgi].Items[ii].Item_stop = now.Unix()*1000+int64(now.Nanosecond()/1000000)

            queue_keys[ ws.job[jgi].Items[ii].Key ] = true
//fmt.Println("got value", key_value, ws.dev_ip, ws.queue)
            debugPub(red, ws, debug, ws.job[jgi].Items[ii].Key, "done OK")
          }
          debugPub(red, ws, debug, fmt.Sprintf("jg@%d", ws.job[jgi].Line), "done")
        } else {
          debugPub(red, ws, debug, fmt.Sprintf("jg@%d", ws.job[jgi].Line), "skip")
        }
      } // JG:
    }

    // lock redis data for this worker

    if err == nil && red != nil {
      debugPub(red, ws, debug, "save", "saving")
      //all job groups done, save data

      keys_args := redis.Args{}.Add(keys_key)
      data_args := redis.Args{}.Add(data_key)

      if ws.queue == 0 {
        keys_args = keys_args.Add("data_ip", fmt.Sprintf("%d:%d:one:str:", sOIDstart, sOIDstop))
        data_args = data_args.Add("data_ip", ws.dev_ip)

        keys_args = keys_args.Add("sysObjectID", fmt.Sprintf("%d:%d:one:oid:", sOIDstart, sOIDstop))
        data_args = data_args.Add("sysObjectID", sysObjectID)
      }

      saving_keys_count := 0

      for jgi := 0; jgi < len(ws.job); jgi++ {
        if !ws.job[jgi].Matched { continue }
        for ii := 0; ii < len(ws.job[jgi].Items); ii++ {
          if ws.job[jgi].Items[ii].Value != nil {

            save_this_key := true
            var key_type string

            switch ws.job[jgi].Items[ii].Value.(type) {
            case string:
              data_args=data_args.Add(ws.job[jgi].Items[ii].Key, ws.job[jgi].Items[ii].Value)
              key_type="one"
            case map[string]string:
              for index, value := range ws.job[jgi].Items[ii].Value.(map[string]string) {
                data_args=data_args.Add(ws.job[jgi].Items[ii].Key+"."+index, value)
              }
              key_type="table"
            default:
              save_this_key = false
            }

            if save_this_key {

              opts := ""
              for opt_const, has_args := range optionArg {
                if (ws.job[jgi].Items[ii].Options & opt_const) != 0 {
                  if opts != "" { opts += "," }
                  opts += const2option[opt_const]
                  if has_args {
                    opts += " "+ws.job[jgi].Items[ii].Opt_values[opt_const]
                  }
                }
              }
              keys_args=keys_args.Add(ws.job[jgi].Items[ii].Key, fmt.Sprintf("%d:%d:%s:%s:%s", ws.job[jgi].Items[ii].Item_start, ws.job[jgi].Items[ii].Item_stop, key_type, const2str[ws.job[jgi].Items[ii].Value_type], opts))
              saving_keys_count++
            }
          }
        }
      }

      keys_args = keys_args.Add("_count", fmt.Sprintf("%d:%d:one:int:", time.Now().Unix()*1000, time.Now().Unix()*1000 + 1))
      data_args = data_args.Add("_count", saving_keys_count)

      keys_args = keys_args.Add("_added", fmt.Sprintf("%d:%d:one:int:", time.Now().Unix()*1000, time.Now().Unix()*1000 + 1))
      data_args = data_args.Add("_added", ws.added.Unix())

      err = redm.Lock(red, time.Second, 10*time.Second)
      if err == nil {

        work_stop := time.Now()
        if work_start.Unix() == work_stop.Unix() {
          work_stop = work_stop.Add(time.Second)
        }

        save_time_i := work_stop.Unix()

        keys_args = keys_args.Add("_time", fmt.Sprintf("%d:%d:one:int:", time.Now().Unix()*1000, time.Now().Unix()*1000 + 1))
        data_args = data_args.Add("_time", save_time_i)

        red.Send("MULTI")
        red.Send("DEL", keys_key)
        red.Send("DEL", data_key)
        red.Send("HSET", keys_args...)
        red.Send("HSET", data_args...)
        red.Send("SET", last_result_key, "ok:"+strconv.FormatInt(work_start.Unix(), 10)+":"+strconv.FormatInt(save_time_i, 10)+":")

        _, err = red.Do("EXEC")

        err = redm.Unlock(red)

        red.Do("PUBLISH", "queue_saved", fmt.Sprintf("%d:%s:scan done", ws.queue, ws.dev_ip))
      }
    }

    if err != nil && err != errInterrupted && red != nil && red.Err() == nil {
      debugPub(red, ws, debug, "error", err.Error())
      red.Do("SET", last_result_key, "error:"+strconv.FormatInt(work_start.Unix(), 10)+":"+strconv.FormatInt(time.Now().Unix(), 10)+":"+err.Error())
    }

    var next_run time.Time

    if err == nil {
      // calculate next run
      for jgi := 0; jgi < len(ws.job); jgi++ {
        if !ws.job[jgi].Matched { continue }
        jg_next_run := ws.job[jgi].Last_run.Add(time.Duration(ws.job[jgi].Refresh)*time.Second)

        if next_run.IsZero() || jg_next_run.Before(next_run) {
          next_run = jg_next_run
        }
      }

      if next_run.IsZero() {
        //nothing to do next??
        next_run = time.Now().Add(IDLE_SLEEP*time.Second)
      }
    } else {
      // some error happened
      next_run = time.Now().Add(ERROR_SLEEP*time.Second)
    }

    if next_run.Before(time.Now()) {
      next_run = time.Now().Add(time.Second)
    }

    if red != nil && red.Err() == nil {
      last_report_time = time.Now()
      if err == nil {
        queue_report := fmt.Sprintf("%d:%d:good_sleep:cycle done", time.Now().Unix(), next_run.Unix())
        red.Do("HSET", queues_key, fmt.Sprint(ws.queue), queue_report)
      } else if err == errInterrupted {
        queue_report := fmt.Sprintf("%d:%d:quit:%s", time.Now().Unix(), 0, err.Error())
        red.Do("HSET", queues_key, fmt.Sprint(ws.queue), queue_report)
      } else {
        queue_report := fmt.Sprintf("%d:%d:error_sleep:%s", time.Now().Unix(), next_run.Unix(), err.Error())
        red.Do("HSET", queues_key, fmt.Sprint(ws.queue), queue_report)
      }
    }

//fmt.Println(ws.dev_ip, ws.queue, "sleep until:", next_run)
//fmt.Println(ws.dev_ip, ws.queue, "sleep for:", next_run.Sub(time.Now()))
//fmt.Println()

    worker_timer := time.NewTimer(next_run.Sub(time.Now()))

    debugPub(red, ws, debug, "worker", fmt.Sprint("sleep until:", " ", next_run))

    for {
      select {
      case command, ok := <-ws.control_ch:
        //some command received
        worker_timer.Stop()
        if !ok || command == "stop" {
          //time to leave
          if red != nil && red.Err() == nil {
            queue_report := fmt.Sprintf("%d:0:quit:bye", time.Now().Unix())
            red.Do("HSET", queues_key, fmt.Sprint(ws.queue), queue_report)
          }
          break WORKER_CYCLE
        } else {
          //check command, decide what to do
          // ignore for now
        }

      case <- worker_timer.C:
        //run next cycle
        continue WORKER_CYCLE
      }
    }
  } //WORKER_CYCLE
  // time to exit
  debugPub(red, ws, debug, "worker", "quit")
}

func read_devlist (red redis.Conn) (M, error) {
  ret := make(M)
  var err error
  var hash map[string]string

  hash, err = redis.StringMap(red.Do("HGETALL", "dev_list"))
  if err != nil { return nil, err }

  for ip, val := range hash {
    a := strings.Split(val, ":")
    if len(a) == 2 && ip_reg.MatchString(ip) && a[1] != "ignore" {
      var t int64
      t, err = strconv.ParseInt(a[0], 10, 64)
      if err == nil  {
        ret[ip] = make(M)
        ret[ip].(M)["time"] = t
        ret[ip].(M)["state"] = a[1]
      }
    }
  }

  return ret, nil
}

func jobs_copy(src []t_scanJobGroup) ([]t_scanJobGroup) {
  ret := make([]t_scanJobGroup, len(src))
  for g, jg := range src {
    ret[g]=jg
    ret[g].Items=make([]t_scanJobItem, len(jg.Items))
    for i, item := range jg.Items {
      ret[g].Items[i]=item
      ret[g].Items[i].Opt_values=make(map[int]string)
      for opt_const, opt_value := range item.Opt_values {
        ret[g].Items[i].Opt_values[opt_const]=opt_value
      }
    }
  }
  return ret
}

func waitTimeout(wg *sync.WaitGroup, timeout time.Duration) bool {
  c := make(chan struct{})
  go func() {
    defer close(c)
    wg.Wait()
  }()

  select {
    case <-c:
      return false // completed normally
    case <-time.After(timeout):
      return true // timed out
  }
}

//scan_match must include *, wich will be checked by index_regex
//example: key.*.suffix , will SCAN 0 MATCH key.*.suffix, will extract with regex ^key\.(.)\.suffix$ and check against index_regex
// return map[string] with type of type_var

func main() {

  var err error

  db_file_record_reg = regexp.MustCompile(DB_FILE_RECORD)

  ip_reg = regexp.MustCompile(IP_REGEX)

  single_run := single.New("gomapper."+red_db) // add redis_db here later

  if err = single_run.CheckLock(); err != nil && err == single.ErrAlreadyRunning {
    log.Fatal("another instance of the app is already running, exiting")
  } else if err != nil {
    // Another error occurred, might be worth handling it as well
    log.Fatalf("failed to acquire exclusive app lock: %v", err)
  }
  defer single_run.TryUnlock()

  sig_ch := make(chan os.Signal, 1)
  signal.Notify(sig_ch, syscall.SIGHUP)
  signal.Notify(sig_ch, syscall.SIGINT)
  signal.Notify(sig_ch, syscall.SIGTERM)
  signal.Notify(sig_ch, syscall.SIGQUIT)

  var wg sync.WaitGroup
  //var err error
  var main_sleep_dur time.Duration
  var main_timer *time.Timer
  var db_devlist M

  data_ch := make(chan t_scanData, 10)
  workers := make(map[string]map[int]*t_workStruct)

  var data t_scanData

  var oids_file_timestamp time.Time

  //var oids_changed bool

  var joblist map[int][]t_scanJobGroup
  var red redis.Conn
  var oids_file_stat os.FileInfo
  var oids_file_md5 string

  var start_set bool=false
  var start_time time.Time

  defer func() {
    if red != nil {
      red.Close()
      red = nil
    }
  } ()

MAIN_LOOP: for {

    err = nil

    cycle_start := time.Now()

    main_sleep_dur=DB_REFRESH_TIME*time.Second

    first_poke_ok := false

    if red != nil && red.Err() == nil {
      //poke redis to check connectivity
      _, err = red.Do("SELECT", red_db)
      if err != nil {
        red.Close()
        red = nil
      } else {
        first_poke_ok = true
      }
    }

    err = nil

    if red != nil && red.Err() != nil {
      red.Close()
      red=nil
    }

    if red == nil {
      red, err = redis.Dial("unix", REDIS_SOCKET)
      first_poke_ok = false
    }

    if err == nil && red != nil && !first_poke_ok {
      //poke redis to check connectivity
      _, err = red.Do("SELECT", red_db)
      if err != nil {
        red.Close()
        red = nil
      }
    }

    globalMutex.Lock()

    if err == nil && red != nil {
      if good_redis < bad_redis {
        good_redis = time.Now().Unix()
        fmt.Fprintln(os.Stderr, "redis is back")
      }
    } else {
      if bad_redis <= good_redis {
        bad_redis = time.Now().Unix()
        fmt.Fprintln(os.Stderr, "redis is down")
      }
    }
    globalMutex.Unlock()

    if err == nil && !start_set {
      start_time=time.Now()
      _, err = red.Do("SET", "gomapper.start", start_time.Unix())
    }

    if err == nil && !start_set {
      start_set = true
    }

    if err == nil && start_set {
      _, err = red.Do("SET", "gomapper.run", fmt.Sprintf("%d:%d", start_time.Unix(), time.Now().Unix()), "EX", 30)
    }

    if err == nil {
      oids_file_stat, err = os.Stat(OIDS_FILE)

      if err == nil {
        if !oids_file_stat.Mode().IsRegular() {
          err=errors.New("Non regular oids file")
        }
      } else {
        err = errors.New("Cannot stat oids file")
      }

      var mtime time.Time
      var file_md5 string

      if err == nil {
        mtime = oids_file_stat.ModTime()
        if mtime.After(oids_file_timestamp) {
          joblist, file_md5, err = read_oids_file()
        }
      }

      if err != nil {
        if bad_oids <= good_oids {
          bad_oids = time.Now().Unix()
          fmt.Fprintln(os.Stderr, err.Error())
        }
      } else {
        if good_oids < bad_oids {
          good_oids = time.Now().Unix()
          fmt.Fprintln(os.Stderr, "Oids file is ok")
        }
      }

      if err == nil && mtime.After(oids_file_timestamp) &&
         file_md5 != oids_file_md5 {
        //if
        queues_list := make([]int, len(joblist))
        qi := 0
        for q, _ := range joblist {
          queues_list[qi]=q
          qi++
        }
        red.Send("MULTI")
        red.Send("DEL", "gomapper.queues")
        red.Send("RPUSH", redis.Args{}.Add("gomapper.queues").Add(start_time.Unix()).AddFlat(queues_list)...)
        _, err = red.Do("EXEC")
        if err == nil {
          oids_file_timestamp = mtime
          oids_file_md5 = file_md5
          //oids_changed = true
          for ip, queues := range workers {
            for _, wd := range queues {
              wd.control_ch <- "stop"
              if wd.conn != nil {
                wd.conn.Close()
              }
              close(wd.control_ch)
            }
            delete(workers, ip)
          }
          if waitTimeout(&wg, 10*time.Second) {
            panic("Waited too long for workers")
          }
        }
      }
    }

    if err == nil {
      db_devlist, err = read_devlist(red)
    }
    if err == nil {
      for ip, _ := range db_devlist {
        if db_devlist.Vs(ip, "state") == "run" {

          _, exists := workers[ip]

          if  exists {
            for q,_ := range workers[ip] {
              workers[ip][q].check=cycle_start
            }
          } else {
            fmt.Println("Adding workers for "+ip)

            ip_queues_key := fmt.Sprintf("ip_queues.%s", ip)

            _, err = red.Do("DEL", ip_queues_key)

            if err == nil {
              for q,_ := range joblist {
                if err == nil {
                  _, err = red.Do("HSET", ip_queues_key, fmt.Sprint(q), fmt.Sprintf("%d:%d:queued:Queued", time.Now().Unix(), time.Now().Unix()))
                }
              }
            }

            if err == nil {
              workers[ip] = make(map[int]*t_workStruct)

              for q,_ := range joblist {
                workers[ip][q]=&t_workStruct{
                  queue:	q,
                  dev_ip:	ip,
                  control_ch:	make(chan string, 1),
                  data_ch:	data_ch,
                  wg:	&wg,
                  added:	cycle_start,
                  check:	cycle_start,
                  job:	jobs_copy(joblist[q]),
                }
                wg.Add(1)
                go worker(workers[ip][q])
              }
            }
          }
        }
      }

      for ip, _ := range workers {
        for q, _ := range workers[ip] {
          if workers[ip][q].check != cycle_start {
            fmt.Println("Killing worker",q,"for",ip)
            workers[ip][q].control_ch <- "stop"
            if workers[ip][q].conn != nil {
              workers[ip][q].conn.Close()
            }
            close(workers[ip][q].control_ch)
            delete(workers[ip],q)
          }
        }
        if len(workers[ip]) == 0 {
          delete(workers, ip)
          if red != nil && red.Err() == nil {
            red.Do("PUBLISH", "queue_saved", "0:"+ip+":deleted or paused")
          }
        }
      }
    } else {
      //fmt.Fprintln(os.Stderr, err.Error())
      main_sleep_dur=DB_ERROR_TIME*time.Second
    }

    if start_set && red != nil && red.Err() == nil {
      run_expire := int64(30)
      if int64(main_sleep_dur/time.Second) > run_expire {
        run_expire = int64(main_sleep_dur/time.Second) + 5
      }
      red.Do("SET", "gomapper.run", fmt.Sprintf("%d:%d", start_time.Unix(), time.Now().Unix()), "EX", run_expire)
    }

    main_timer=time.NewTimer(main_sleep_dur)

    for {
      select {
        case s := <-sig_ch:
          main_timer.Stop()
          fmt.Println("\nmain got signal")
          if s == syscall.SIGHUP || s == syscall.SIGUSR1 {
            continue MAIN_LOOP
          }
          break MAIN_LOOP
        case <-main_timer.C:
          //runtime.GC()
          mu := GetMemUsage()
          if red != nil && red.Err() == nil {
            red.Do("SET", "gomapper.memstat", mu)
          }
          //fmt.Printf("\033[1;33m%v\033[0m\n", mu)
          continue MAIN_LOOP
        case data = <-data_ch:
      }
      //we've got data

      //process data
      //fmt.Println("Got data "+data.data_str)

      switch data.data_type {
        case dtExit:
          //worker self-destructed
          ws := *data.ws
          if ws.conn != nil {
            ws.conn.Close()
          }
          _, exists := workers[ws.dev_ip]
          if exists {
            _, q_exists := workers[ws.dev_ip][ws.queue]
            if q_exists {
              delete(workers[ws.dev_ip], ws.queue)
            }
            if len(workers[ws.dev_ip]) == 0 {
              delete(workers, ws.dev_ip)
            }
          }
        default:
          //do nothing
      }
    }
  }

  fmt.Println("main stopping workers")
  for ip, queues := range workers {
    for _, wd := range queues {
      wd.control_ch <- "stop"
      if wd.conn != nil {
        wd.conn.Close()
      }
      close(wd.control_ch)
    }
    delete(workers, ip)
  }

  fmt.Println("main waiting for workers to stop")
  waitTimeout(&wg, 60*time.Second)
  if red != nil && red.Err() == nil {
    red.Do("DEL", "gomapper.run")
  }
  fmt.Println("main done")
}
