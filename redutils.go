package main

import (
  "github.com/gomodule/redigo/redis"
  "regexp"
  "strings"
  "strconv"
  "errors"
)

func getRedKeys(red redis.Conn, scan_match string) ([]string, error) {
  var ret []string
  var err error

  ret_keys := make(map[string]bool)

  cursor := "0"

  for {
    var red_ret interface{}
    red_ret, err = red.Do("SCAN", cursor, "MATCH", scan_match)
    if err != nil { return nil, err }
    //fmt.Printf("%T: %v\n", red_ret, red_ret)

    //spew.Dump(red_ret)

    scan_res, ok := red_ret.([]interface{})
    if !ok {
      return nil, errors.New("Type error from SCAN")
    }
    if len(scan_res) != 2 { return nil, errors.New("Bad answer from SCAN") }

    cursor, _ = redis.String(scan_res[0], nil)
    keys, _ := redis.Strings(scan_res[1], nil)

    for _, key := range keys {
      _, exist := ret_keys[key]
      if !exist {
        ret_keys[key]=true
        ret = append(ret, key)
      }
    }

    if(cursor == "0") { break }
  }
  return ret, nil
}


func getRedTable(red redis.Conn, scan_match string, index_regex *regexp.Regexp, type_var interface{}) (map[string]interface{}, error) {
  var err error

  asterisk_index := strings.Index(scan_match, "*")
  if asterisk_index < 0 { return nil, errors.New("No asterisk in scan_match") }

  key_reg, reg_err := regexp.Compile("^"+regexp.QuoteMeta(scan_match[:asterisk_index])+"(.*)"+regexp.QuoteMeta(scan_match[asterisk_index+1:])+"$")
  if reg_err != nil { return nil, reg_err }

  ret := make(map[string]interface{})

  var keys []string

  keys, err = getRedKeys(red, scan_match)
  if err != nil { return nil, err }

  var key_values []string
  key_values, err = redis.Strings(red.Do("MGET", redis.Args{}.AddFlat(keys)...))
  if err != nil { return nil, err }

  if len(keys) != len(key_values) { return nil, errors.New("MGET result length mismatch") }

  var conv_int int64
  var conv_uint uint64
  var conv_float float64

  for i, key := range keys {
    match := key_reg.FindStringSubmatch(key)
    if match != nil && len(match) == 2 && index_regex.MatchString(match[1]) {
      index := match[1]
      switch type_var.(type) {
      case string:
        //
      case int:
        conv_int, err = strconv.ParseInt(key_values[i], 10, 32)
      case uint:
        conv_uint, err = strconv.ParseUint(key_values[i], 10, 32)
      case int32:
        conv_int, err = strconv.ParseInt(key_values[i], 10, 32)
      case uint32:
        conv_uint, err = strconv.ParseUint(key_values[i], 10, 32)
      case int64:
        conv_int, err = strconv.ParseInt(key_values[i], 10, 64)
      case uint64:
        conv_uint, err = strconv.ParseUint(key_values[i], 10, 64)
      case float64:
        conv_float, err = strconv.ParseFloat(key_values[i], 64)
      default:
        return nil, errors.New("Unsupported type")
      }
      if err != nil {
        return nil, err
      }
      switch type_var.(type) {
      case string:
        ret[ index ] = key_values[i]
      case int:
        ret[ index ] = int(conv_int)
      case uint:
        ret[ index ] = uint(conv_uint)
      case int32:
        ret[ index ] = int32(conv_int)
      case uint32:
        ret[ index ] = uint32(conv_uint)
      case int64:
        ret[ index ] = int64(conv_int)
      case uint64:
        ret[ index ] = uint64(conv_uint)
      case float64:
        ret[ index ] = conv_float
      default:
        return nil, errors.New("Unsupported type")
      }
    }

  }
  return ret, nil
}


