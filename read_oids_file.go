package main

import (
  _ "github.com/gomodule/redigo/redis"
  "fmt"
  "os"
  "bufio"
  "regexp"
  "strings"
  "strconv"
  "errors"
  _ "time"
  _ "encoding/json"
  "crypto/md5"
  "io"
  "encoding/hex"
)

var read_oids_file_define_regex *regexp.Regexp
var read_oids_file_match_regex *regexp.Regexp
var read_oids_file_item_regex *regexp.Regexp
var read_oids_file_var_regex *regexp.Regexp

func read_oids_file() (map[int][]t_scanJobGroup, string, error) {
  ret := make(map[int][]t_scanJobGroup)

  md5_hasher := md5.New()

  defines := make(map[string]string)

  file, err := os.Open(OIDS_FILE)
  if err != nil { return nil, "", err }

  defer file.Close()

  scanner := bufio.NewScanner(file)

  var current_queue int = 0
  var current_jg = t_scanJobGroup{ Refresh: 60, Match_type: -1, Unmatch_type: mtNone, Items: []t_scanJobItem{} }
  var end_signalled bool=false
  var first_jg bool=true

  var current_line_num int=0

  var queue_keys = make(map[string]int)

  for scanner.Scan() {
    current_line_num++
    line := strings.Trim(scanner.Text(), " \t")

    if line == "" || strings.Index(line, "#") == 0 {
      continue
    }

    io.WriteString(md5_hasher, line)

    if strings.Index(line, "define") == 0 {
      if read_oids_file_define_regex == nil {
        read_oids_file_define_regex, err = regexp.Compile(`^define\s+(\S+)\s+((?:\.\d+)+)$`)
        if err != nil { return nil, "", err }
      }
      m := read_oids_file_define_regex.FindStringSubmatch(line)
      if m != nil {
        defines["{"+m[1]+"}"]=m[2]
      }
    } else if strings.Index(line, "queue") == 0 {
      if current_jg.Match_type == -1 { return nil, "", errors.New(fmt.Sprintf("No job group to set queue in OIDs file at %d", current_line_num)) }
      str := strings.Trim(line[len("queue"):]," \t")
      current_queue, err = strconv.Atoi(str)
      if err != nil { return nil, "", errors.New(fmt.Sprintf("%s, in OIDs file at %d", err.Error(), current_line_num)) }
    } else if strings.Index(line, "refresh") == 0 {
      str := strings.Trim(line[len("refresh"):]," \t")
      current_jg.Refresh, err = strconv.Atoi(str)
      if err != nil { return nil, "", errors.New(fmt.Sprintf("%s, in OIDs file at %d", err.Error(), current_line_num)) }
    } else if line == "end" || string(line[0]) == "*" || string(line[0]) == "~" || string(line[0]) == "=" || string(line[0]) == "^" {
      if !first_jg {
        if current_jg.Match_type == -1 {
          return nil, "", errors.New(fmt.Sprintf("OIDs file empty or invalid, read %d lines", current_line_num))
        }
        if len(current_jg.Items) > 0 || true {
          ret[current_queue]=append(ret[current_queue], current_jg)
        } else {
          return nil, "", errors.New(fmt.Sprintf("Empty job group in OIDs file before %d", current_line_num))
        }
        current_jg=t_scanJobGroup{ Refresh: 60, Match_type: -1, Unmatch_type: mtNone, Items: []t_scanJobItem{} }
        current_queue = 0
      } else {
        if line == "end" {
          return nil, "", errors.New(fmt.Sprintf("OIDs file empty or invalid, read %d lines untill end", current_line_num))
        }
      }

      if line == "end" {
        end_signalled = true
        break
      }

      current_jg.Line = current_line_num

      first_jg = false
      if read_oids_file_match_regex == nil {
        read_oids_file_match_regex, err = regexp.Compile(`^(=|~|\^|\*)(\S*)(?:\s+(=|~|\^)(\S+))?$`)
        if err != nil { return nil, "", err }
      }

      if read_oids_file_var_regex == nil {
        read_oids_file_var_regex, err = regexp.Compile(`{[^}]*}`)
        if err != nil { return nil, "", err }
      }

      var missing_vars []string

      m := read_oids_file_match_regex.FindStringSubmatch(line)
      if m == nil { return nil, "", errors.New(fmt.Sprintf("Job group syntax error in OIDs file at %d", current_line_num)) }

      match_op := m[1]
      match_str := m[2]

      unmatch_op := m[3]
      unmatch_str := m[4]

      if match_op == "~" {
        missing_vars = make([]string, 0)
        match_str = read_oids_file_var_regex.ReplaceAllStringFunc(match_str, func(a string) string {
          replacement, found := defines[a]; if !found { missing_vars = append(missing_vars, a); return "" } else { return regexp.QuoteMeta(replacement) }
        })

        if len(missing_vars) > 0 {
          return nil, "", errors.New(fmt.Sprintf("Missing vars %v in OIDs file at %d", missing_vars, current_line_num))
        }

        _, err = regexp.Compile(match_str)
        if err != nil {
          return nil, "", errors.New(fmt.Sprintf("Invalid regexp in OIDs file at %d", current_line_num))
        }

        current_jg.Match_type = mtRegex
        current_jg.Match_str = match_str
      } else if match_op == "^" || match_op == "=" {
        missing_vars = make([]string, 0)
        match_str = read_oids_file_var_regex.ReplaceAllStringFunc(match_str, func(a string) string {
          replacement, found := defines[a]; if !found { missing_vars = append(missing_vars, a); return "" } else { return replacement }
        })

        if len(missing_vars) > 0 {
          return nil, "", errors.New(fmt.Sprintf("Missing vars %v in OIDs file at %d", missing_vars, current_line_num))
        }

        if match_op == "^" {
          current_jg.Match_type = mtPrefix
          current_jg.Match_str = match_str+"."
        } else {
          current_jg.Match_type = mtExact
          current_jg.Match_str = match_str
        }
      } else if match_op == "*" {
        if match_str != "" {
          return nil, "", errors.New(fmt.Sprintf("Job group syntax error in OIDs file at %d", current_line_num))
        }
        current_jg.Match_type = mtAny
      } else {
        return nil, "", errors.New("Programm error: unknown match type")
      }

      if unmatch_op == "~" {
        missing_vars = make([]string, 0)
        unmatch_str = read_oids_file_var_regex.ReplaceAllStringFunc(unmatch_str, func(a string) string {
          replacement, found := defines[a]; if !found { missing_vars = append(missing_vars, a); return "" } else { return regexp.QuoteMeta(replacement) }
        })

        if len(missing_vars) > 0 {
          return nil, "", errors.New(fmt.Sprintf("Missing vars %v in OIDs file at %d", missing_vars, current_line_num))
        }

        _, err = regexp.Compile(unmatch_str)
        if err != nil {
          return nil, "", errors.New(fmt.Sprintf("Invalid regexp in OIDs file at %d", current_line_num))
        }

        current_jg.Unmatch_type = mtRegex
        current_jg.Unmatch_str = unmatch_str
      } else if unmatch_op == "^" || unmatch_op == "=" {
        missing_vars = make([]string, 0)
        unmatch_str = read_oids_file_var_regex.ReplaceAllStringFunc(unmatch_str, func(a string) string {
          replacement, found := defines[a]; if !found { missing_vars = append(missing_vars, a); return "" } else { return replacement }
        })

        if len(missing_vars) > 0 {
          return nil, "", errors.New(fmt.Sprintf("Missing vars %v in OIDs file at %d", missing_vars, current_line_num))
        }

        if unmatch_op == "^" {
          current_jg.Unmatch_type = mtPrefix
          current_jg.Unmatch_str = unmatch_str+"."
        } else {
          current_jg.Unmatch_type = mtExact
          current_jg.Unmatch_str = unmatch_str
        }
      } else if unmatch_op == "" {
        current_jg.Unmatch_type = mtNone
      } else {
        return nil, "", errors.New("Programm error: unknown unmatch type \""+unmatch_op+"\"")
      }
    } else if  strings.Index(line, "table") == 0 || strings.Index(line, "one") == 0 {
      if read_oids_file_item_regex == nil {
        read_oids_file_item_regex, err = regexp.Compile(`^(table|one)\s+(hex|str|int|uns)\s+([a-zA-Z0-9_\-]+)\s+((?:\.\d+)+)(?:\s+([^,\s]+(?:,[^,\s]+)*)(?:\s+([^,\s]+(?:,[^,\s]+)*))?)?$`)
        if err != nil { return nil, "", err }
      }

      m := read_oids_file_item_regex.FindStringSubmatch(line)
      if m == nil { return nil, "", errors.New(fmt.Sprintf("Item syntax error in OIDs file at %d", current_line_num)) }

      item := t_scanJobItem{ Line: current_line_num, Key: m[3], Oid: m[4], Opt_values: make(map[int]string) }

      key_queue, key_found := queue_keys[m[3]]

      if key_found && key_queue != current_queue {
        return nil, "", errors.New(fmt.Sprintf("Key redefined in another queue in OIDs file at %d", current_line_num))
      }

      switch m[1] {
      case "table":
        item.Item_type = itTable
      case "one":
        item.Item_type = itOne
      }

      switch m[2] {
      case "hex":
        item.Value_type = vtHex
      case "str":
        item.Value_type = vtString
      case "int":
        item.Value_type = vtInt
      case "uns":
        item.Value_type = vtUns
      }

      var item_options []string
      options_values := strings.Split(m[6], ",")

      if m[5] != "" {
        item_options = strings.Split(m[5], ",")
        for _, option := range item_options {
          opt_const, opt_valid := option2const[option]
          if !opt_valid {
            return nil, "", errors.New(fmt.Sprintf("Unknown item option \"%s\" in OIDs file at %d", option, current_line_num))
          }
          if (item.Options & opt_const) != 0 {
            return nil, "", errors.New(fmt.Sprintf("Duplicate item option \"%s\" in OIDs file at %d", option, current_line_num))
          }
          item.Options = item.Options | opt_const

          opt_has_arg, opt_valid := optionArg[opt_const]
          if !opt_valid {
            return nil, "", errors.New(fmt.Sprintf("Unknown item option \"%s\" arg in OIDs file at %d", option, current_line_num))
          }

          if opt_has_arg {
            if len(options_values) == 0 || options_values[0] == "" {
              return nil, "", errors.New(fmt.Sprintf("Item option \"%s\" arg missing in OIDs file at %d", option, current_line_num))
            }
            item.Opt_values[opt_const], options_values = options_values[0], options_values[1:]
          }
        }
      }

      current_jg.Items=append(current_jg.Items, item)

      queue_keys[m[3]] = current_queue

    } else {
      return nil, "", errors.New(fmt.Sprintf("Unsupported command in OIDs file at %d", current_line_num))
    }
  }

  if !end_signalled { return nil, "", errors.New("No end command reached") }

  sum := md5_hasher.Sum(nil)

  return ret, hex.EncodeToString(sum), nil
}
