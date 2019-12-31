package main

import (
  snmp "github.com/soniah/gosnmp"
  "encoding/hex"
  "strings"
  "strconv"
  "errors"
  "time"
  "fmt"

  w "github.com/jimlawless/whereami"
)

func init() {
  w.WhereAmI()
  _ = time.Now()
}

func timeTicks2str(ticks uint) string {
  var usec uint=ticks%100
  ticks= ticks / 100;
  var sec uint= ticks % 60;
  ticks= ticks / 60;
  var min uint= ticks % 60;
  ticks= ticks / 60;
  var hours uint= ticks % 24;
  var days uint= ticks / 24;
  return fmt.Sprintf("%d days, %02d:%02d:%02d.%02d", days, hours, min, sec, usec);
}

func decodeVariable(variable snmp.SnmpPDU, expect_type int) (string, error) {
  switch variable.Type {

  case snmp.NoSuchInstance, snmp.NoSuchObject:
    return "", errors.New("NoSuchInstance")

  case snmp.OctetString:
    switch expect_type {
    case vtString:
      return strings.TrimRight(string(variable.Value.([]byte)), "\x00"), nil
    case vtHex:
      return hex.EncodeToString(variable.Value.([]byte)), nil
    case vtUns:
      _, sconverr := strconv.ParseUint(strings.TrimRight(string(variable.Value.([]byte)), "\x00"), 10, 64)
      if sconverr != nil {
        return "", errors.New("Non numeric string.")
      } else {
        return string(variable.Value.([]byte)), nil
      }
    case vtInt:
      _, sconverr := strconv.Atoi(strings.TrimRight(string(variable.Value.([]byte)), "\x00"))
      if sconverr != nil {
        return "", errors.New("Non numeric string.")
      } else {
        return string(variable.Value.([]byte)), nil
      }
    }

  case snmp.ObjectIdentifier:
    switch expect_type {
    case vtString, vtOid:
      return variable.Value.(string), nil
    }

  case snmp.IPAddress:
    switch expect_type {
    case vtString:
      return variable.Value.(string), nil
    }

  case snmp.TimeTicks:
    switch expect_type {
    case vtString:
      return timeTicks2str(variable.Value.(uint)), nil
    case vtInt, vtUns:
      return strconv.FormatUint(uint64(variable.Value.(uint)), 10), nil
    }

  case snmp.Integer:
    switch expect_type {
    case vtString, vtInt, vtUns:
      return strconv.FormatInt(int64(variable.Value.(int)), 10), nil
    case vtHex:
      return strconv.FormatInt(int64(variable.Value.(int)), 16), nil
    }

  case snmp.Counter32, snmp.Gauge32:
    switch expect_type {
    case vtString, vtInt, vtUns:
      return strconv.FormatUint(uint64(variable.Value.(uint)), 10), nil
    case vtHex:
      return strconv.FormatUint(uint64(variable.Value.(uint)), 16), nil
    }

  case snmp.Counter64:
    switch expect_type {
    case vtString, vtInt, vtUns:
      return strconv.FormatUint(variable.Value.(uint64), 10), nil
    case vtHex:
      return strconv.FormatUint(variable.Value.(uint64), 16), nil
    }
  }

  return "", errors.New(fmt.Sprintf("Cannot convert value type %v, value %v, to to expected type %s, oid %s", variable.Type, variable.Value, const2str[expect_type], variable.Name))
}

func getOne(client *snmp.GoSNMP, oid string, expect_type int) (string, error) {
  res, err := client.Get([]string{oid})
  if err != nil {
    return "", err
  }

  if res.Error != snmp.NoError {
    return "", errors.New(fmt.Sprintf("Error in PDU: %v", res.Error))
  }

  if len(res.Variables) != 1 {
    return "", errors.New("getOne: No variables returned")
  }

  if(res.Variables[0].Name != oid) {
    return "", errors.New("getOne: different variable returned: "+res.Variables[0].Name)
  }

  return decodeVariable(res.Variables[0], expect_type)
}

type callback func()

func getTableFunc(client *snmp.GoSNMP, oid string, expect_type int, report callback) (map[string]string, error) {
  ret := make(map[string]string)

  next_oid := oid
  cut_len := len(oid+".")

  var res *snmp.SnmpPacket
  var err error

  for res, err = client.GetBulk([]string{next_oid}, 0, 10); err == nil; res, err = client.GetBulk([]string{next_oid}, 0, 10) {
    if res.Error != snmp.NoError {
      return nil, errors.New(fmt.Sprintf("Error in PDU: %v", res.Error))
    }

    report()

    var vars_matched int=0
    var done bool=false

    var last_oid string=""

    for _, variable := range res.Variables {

      if strings.HasPrefix(variable.Name, oid+".") {
        index := variable.Name[cut_len:]
        _, found := ret[index]
        if !found {
          ret[index], err = decodeVariable(variable, expect_type)
          if err != nil {
            return nil, err
          }
          vars_matched++
          last_oid = variable.Name
        } else { //!!
          // remote agent running in circles, stop
          done=true
          break
        }
      } else {
        //table end
        done=true
        break
      }
    }
    if vars_matched == 0 || done {
      break
    } else {
      next_oid=last_oid
    }
  }
  if err != nil {
    return nil, err
  }
  if len(ret) == 0 {
    return nil, errors.New("NoSuchInstance")
  }
  return ret, nil
}
