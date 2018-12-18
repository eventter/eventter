package main

import (
	"bytes"
	"encoding/xml"
	"go/format"
	"io/ioutil"
	"log"
	"os"
	"strings"
	"text/template"

	"github.com/pkg/errors"
)

type Root struct {
	InputFilename  string
	OutputFilename string
	Major          int         `xml:"major,attr"`
	Minor          int         `xml:"minor,attr"`
	Revision       int         `xml:"revision,attr"`
	Port           int         `xml:"port,attr"`
	Constants      []*Constant `xml:"constant"`
	Domains        []*Domain   `xml:"domain"`
	Classes        []*Class    `xml:"class"`
}

type Constant struct {
	Name  string `xml:"name,attr"`
	Value int    `xml:"value,attr"`
	Class string `xml:"class,attr"`
}

type Domain struct {
	Name    string          `xml:"name,attr"`
	Type    string          `xml:"type,attr"`
	Asserts []*DomainAssert `xml:"assert"`
}

type DomainAssert struct {
	Check  string `xml:"check,attr"`
	Method string `xml:"method,attr"`
	Value  string `xml:"value,attr"`
}

type Class struct {
	Name    string     `xml:"name,attr"`
	Handler string     `xml:"handler,attr"`
	Index   int        `xml:"index,attr"`
	Chassis []*Chassis `xml:"chassis"`
	Fields  []*Field   `xml:"field"`
	Methods []*Method  `xml:"method"`
}

type Chassis struct {
	Name      string `xml:"name,attr"`
	Implement string `xml:"implement,attr"`
}

type Method struct {
	Name        string            `xml:"name,attr"`
	Synchronous bool              `xml:"synchronous,attr"`
	Content     bool              `xml:"content,attr"`
	Deprecated  bool              `xml:"deprecated,attr"`
	Index       int               `xml:"index,attr"`
	Chassis     []*Chassis        `xml:"chassis"`
	Responses   []*MethodResponse `xml:"response"`
	Fields      []*Field          `xml:"field"`
}

type MethodResponse struct {
	Name string `xml:"name,attr"`
}

type Field struct {
	Name    string          `xml:"name,attr"`
	Type    string          `xml:"type,attr"`
	Domain  string          `xml:"domain,attr"`
	Asserts []*DomainAssert `xml:"assert"`
}

func main() {
	if err := run(); err != nil {
		log.Fatal(err)
	}
}

var tpl = template.Must(template.New("tpl").Funcs(map[string]interface{}{
	"convertCase": func(s string) string {
		parts := strings.Split(s, "-")
		for i, part := range parts {
			if part == "id" {
				parts[i] = "ID"
			} else {
				parts[i] = strings.ToUpper(part[:1]) + part[1:]
			}
		}
		return strings.Join(parts, "")
	},
	"goType": func(t string) string {
		switch t {
		case "bit":
			return "bool"
		case "octet":
			return "uint8"
		case "short":
			return "uint16"
		case "long":
			return "uint32"
		case "longlong":
			return "uint64"
		case "shortstr":
			fallthrough
		case "longstr":
			return "string"
		case "table":
			return "*types.Struct"
		case "timestamp":
			return "time.Time"
		default:
			panic("unhandled type: " + t)
		}
	},
	"join": func(s ...string) string {
		return strings.Join(s, "")
	},
	"panic": func(v interface{}) string {
		panic(v)
	},
	"inc": func(n int) int {
		return n + 1
	},
	"in": func(needle string, haystack ...string) bool {
		for _, hay := range haystack {
			if hay == needle {
				return true
			}
		}

		return false
	},
}).Parse(`
{{- define "marshalField" -}}
{{- $goFieldName := .Name|convertCase -}}
{{- if eq .Type "octet" }}
	buf.WriteByte(f.{{ $goFieldName }})
{{- else if eq .Type "short" }}
	endian.PutUint16(x[:2], f.{{ $goFieldName }})
	buf.Write(x[:2])
{{- else if eq .Type "long" }}
	endian.PutUint32(x[:4], f.{{ $goFieldName }})
	buf.Write(x[:4])
{{- else if eq .Type "longlong" }}
	endian.PutUint64(x[:8], f.{{ $goFieldName }})
	buf.Write(x[:8])
{{- else if eq .Type "shortstr" }}
	if l := len(f.{{ $goFieldName }}); l > math.MaxUint8 {
		return nil, errors.Errorf("{{ .Name }} can be at most %d bytes long, got %d bytes", math.MaxUint8, l)
	} else {
		buf.WriteByte(byte(l))
		buf.WriteString(f.{{ $goFieldName }})
	}
{{- else if eq .Type "longstr" }}
	if l := len(f.{{ $goFieldName }}); l > math.MaxUint32 {
		return nil, errors.Errorf("{{ .Name }} can be at most %d bytes long, got %d bytes", math.MaxUint32, l)
	} else {
		endian.PutUint32(x[:4], uint32(l))
		buf.Write(x[:4])
		buf.WriteString(f.{{ $goFieldName }})
	}
{{- else if eq .Type "table" }}
	if tableBuf, err := marshalTable(f.{{ $goFieldName }}); err != nil {
		return nil, errors.Wrap(err, "{{ .Name }} table marshal failed")
	} else {
		endian.PutUint32(x[:4], uint32(len(tableBuf)))
		buf.Write(x[:4])
		buf.Write(tableBuf)
	}
{{- else if eq .Type "timestamp" }}
	endian.PutUint64(x[:8], uint64(f.{{ $goFieldName }}.Unix()))
	buf.Write(x[:8])
{{- else -}}
	{{- panic (printf "unhandled type %s" .Type) -}}
{{- end }}
{{- end -}}

{{- define "unmarshalField" -}}
{{- $goFieldName := .Name|convertCase -}}
{{- if eq .Type "octet" }}
	if b, err := buf.ReadByte(); err != nil {
		return errors.Wrap(err, "field {{ .Name }}: read octet failed")
	} else {
		f.{{ $goFieldName }} = b
	}
{{- else if eq .Type "short" }}
	if n, err := buf.Read(x[:2]); err != nil {
		return errors.Wrap(err, "field {{ .Name }}: read short failed")
	} else if n < 2 {
		return errors.New("field {{ .Name }}: read short failed")
	}
	f.{{ $goFieldName }} = endian.Uint16(x[:2])
{{- else if eq .Type "long" }}
	if n, err := buf.Read(x[:4]); err != nil {
		return errors.Wrap(err, "field {{ .Name }}: read long failed")
	} else if n < 4 {
		return errors.New("field {{ .Name }}: read long failed")
	}
	f.{{ $goFieldName }} = endian.Uint32(x[:4])
{{- else if eq .Type "longlong" }}
	if n, err := buf.Read(x[:8]); err != nil {
		return errors.Wrap(err, "field {{ .Name }}: read longlong failed")
	} else if n < 8 {
		return errors.New("field {{ .Name }}: read longlong failed")
	}
	f.{{ $goFieldName }} = endian.Uint64(x[:8])
{{- else if eq .Type "shortstr" }}
	if b, err := buf.ReadByte(); err != nil {
		return errors.Wrap(err, "field {{ .Name }}: read shortstr failed")
	} else {
		l := int(b)
		s := buf.Next(l)
		if len(s) < l {
			return errors.New("field {{ .Name }}: read shortstr failed")
		}
		f.{{ $goFieldName }} = string(s)
	}
{{- else if eq .Type "longstr" }}
	if n, err := buf.Read(x[:4]); err != nil {
		return errors.Wrap(err, "field {{ .Name }}: read longstr failed")
	} else if n < 4 {
		return errors.New("field {{ .Name }}: read longstr failed")
	} else {
		l := int(endian.Uint32(x[:4]))
		s := buf.Next(l)
		if len(s) < l {
			return errors.New("field {{ .Name }}: read longstr failed")
		}
		f.{{ $goFieldName }} = string(s)
	}
{{- else if eq .Type "table" }}
	if n, err := buf.Read(x[:4]); err != nil {
		return errors.Wrap(err, "field {{ .Name }}: read table failed")
	} else if n < 4 {
		return errors.New("field {{ .Name }}: read table failed")
	} else {
		l := int(endian.Uint32(x[:4]))
		b := buf.Next(l)
		if len(b) < l {
			return errors.New("field {{ .Name }}: read table failed")
		}
		if f.{{ $goFieldName }}, err = unmarshalTable(b); err != nil {
			return errors.Wrap(err, "field {{ .Name }}: read table failed")
		}
	}
{{- else if eq .Type "timestamp" }}
	if n, err := buf.Read(x[:8]); err != nil {
		return errors.Wrap(err, "field {{ .Name }}: read timestamp failed")
	} else if n < 8 {
		return ErrShortRead
	}
	f.{{ $goFieldName }} = time.Unix(int64(endian.Uint64(x[:8])), 0)
{{- else -}}
	{{- panic (printf "%s - unhandled type %s" .Name .Type) -}}
{{- end }}
{{- end -}}

{{- block "root" . }}
// Code generated by ./generator/main.go. DO NOT EDIT.
package v{{ .Major }}

//go:generate go run ./generator/main.go {{ .InputFilename }} {{ .OutputFilename }}

import (
	"bytes"
	"encoding/binary"
	"math"
	"time"

	"github.com/gogo/protobuf/types"
	"github.com/pkg/errors"
)

const (
	Major = {{ .Major }}
	Minor = {{ .Minor }}
	Revision = {{ .Revision }}
	Port = {{ .Port }}
{{- range .Constants }}
	{{ .Name|convertCase }} = {{ .Value }}
{{- end }}
{{- range $class := .Classes }}

	{{ $class.Name|convertCase }}Class = {{ $class.Index }}
	{{- range $method := .Methods }}
		{{ $class.Name|convertCase }}{{ $method.Name|convertCase }}Method = {{ $method.Index }}
	{{- end }}
{{- end }}
)

var endian = binary.BigEndian

{{ range $class := .Classes }}
{{ if gt ($class.Fields|len) 0 }}
type ContentHeader struct {
	ClassID uint16
	Weight uint16
	BodySize uint64
	{{- range $field := $class.Fields }}
	{{ $field.Name|convertCase }} {{ $field.Type|goType }}
	{{- end }}
}

func (f *ContentHeader) Unmarshal(buf []byte) error {
	panic("implement me")
}

func (f *ContentHeader) Marshal() ([]byte, error) {
	var x [8]byte
	_ = x
	var flags uint16
	buf := bytes.Buffer{}
	{{- range $index, $field := $class.Fields }}
		{{ $goFieldName := $field.Name|convertCase }}
		{{- if in $field.Type "octet" "short" "long" "longlong" }}
			if f.{{ $goFieldName }} > 0 {
		{{- else if in $field.Type "shortstr" "longstr" }}
			if f.{{ $goFieldName }} != "" {
		{{- else if in $field.Type "table" }}
			if f.{{ $goFieldName }} != nil {
		{{- else if in $field.Type "timestamp" }}
			if !f.{{ $goFieldName }}.IsZero() {
		{{- else -}}
			{{- panic (printf "unhandled type %s" $field.Type) -}}
		{{- end -}}
			flags |= 1 << {{ $index }}
			{{ template "marshalField" $field }}
		}
	{{- end }}

	ret := bytes.Buffer{}
	endian.PutUint16(x[:2], f.ClassID)
	ret.Write(x[:2])
	endian.PutUint16(x[:2], f.Weight)
	ret.Write(x[:2])
	endian.PutUint64(x[:8], f.BodySize)
	ret.Write(x[:8])
	endian.PutUint16(x[:2], flags)
	ret.Write(x[:2])
	ret.Write(buf.Bytes())

	return ret.Bytes(), nil
}

{{ end }}
{{ range $method := .Methods }}
{{ $frame := join ($class.Name|convertCase) ($method.Name|convertCase) }}
type {{ $frame }} struct {
	{{- range $field := $method.Fields }}
	{{ $field.Name|convertCase }} {{ $field.Type|goType }}
	{{- end }}
}

func (f *{{ $frame }}) Unmarshal(data []byte) error {
	*f = {{ $frame }}{}
	var x [8]byte
	_ = x
	buf := bytes.NewBuffer(data)

	if n, err := buf.Read(x[:2]); err != nil {
		return errors.Wrap(err, "read class ID failed")
	} else if n < 2 {
		return errors.New("read class ID failed")
	}
	if id := endian.Uint16(x[:2]); id != {{ $class.Name|convertCase }}Class {
		return errors.Errorf("expected class ID %d, got %d", {{ $class.Name|convertCase }}Class, id)
	}
	if n, err := buf.Read(x[:2]); err != nil {
		return errors.Wrap(err, "read method ID failed")
	} else if n < 2 {
		return errors.New("read method ID failed")
	}
	if id := endian.Uint16(x[:2]); id != {{ $class.Name|convertCase }}{{ $method.Name|convertCase }}Method {
		return errors.Errorf("expected method ID %d, got %d", {{ $class.Name|convertCase }}{{ $method.Name|convertCase }}Method, id)
	}
	{{- $bitFields := 0 }}
	{{- range $field := $method.Fields }}
		{{- $goFieldName := $field.Name|convertCase -}}
		{{- if eq $field.Type "bit" }}
			{{ $bitFields = inc $bitFields }}
		{{- else -}}
			{{- if gt $bitFields 0 -}}
				if b, err := buf.ReadByte(); err != nil {
					return errors.Wrap(err, "read bits failed")
				} else {
					_ = b
					// TODO: bit fields
				}
				{{- $bitFields = 0 -}}
			{{- end }}
			{{- template "unmarshalField" $field }}
		{{- end -}}
	{{- end }}
	{{- if gt $bitFields 0 -}}
		// TODO: end bit fields
		{{- $bitFields = 0 -}}
	{{- end }}
	return nil
}

func (f *{{ $frame }}) Marshal() ([]byte, error) {
	var x [8]byte
	_ = x
	var bits byte = 0
	_ = bits
	buf := bytes.Buffer{}
	endian.PutUint16(x[:2], {{ $class.Name|convertCase }}Class)
	buf.Write(x[:2])
	endian.PutUint16(x[:2], {{ $class.Name|convertCase }}{{ $method.Name|convertCase }}Method)
	buf.Write(x[:2])
	{{- $bitFields := 0 }}
	{{- range $field := $method.Fields }}
		{{- $goFieldName := $field.Name|convertCase -}}
		{{- if eq $field.Type "bit" }}
			if f.{{ $goFieldName }} {
				bits |= 1 << {{ $bitFields }}
			}
			{{ $bitFields = inc $bitFields }}
		{{- else -}}
			{{- if gt $bitFields 0 -}}
				buf.WriteByte(bits)
				bits = 0
				{{- $bitFields = 0 -}}
			{{- end }}
			{{- template "marshalField" $field }}
		{{- end -}}
	{{- end }}
	{{- if gt $bitFields 0 -}}
		buf.WriteByte(bits)
		bits = 0
		{{- $bitFields = 0 -}}
	{{- end }}
	return buf.Bytes(), nil
}
{{ end }}
{{ end }}

{{ end }}
`))

func run() error {
	root := &Root{
		InputFilename:  os.Args[1],
		OutputFilename: os.Args[2],
	}

	data, err := ioutil.ReadFile(root.InputFilename)
	if err != nil {
		return errors.Wrap(err, "read failed")
	}

	if err := xml.Unmarshal(data, root); err != nil {
		return errors.Wrap(err, "unmarshal failed")
	}

	if err := resolveTypes(root); err != nil {
		return errors.Wrap(err, "resolve failed")
	}

	buffer := bytes.Buffer{}

	if err := tpl.Execute(&buffer, root); err != nil {
		return errors.Wrap(err, "template failed")
	}

	output, err := format.Source(buffer.Bytes())
	if err != nil {
		os.Stdout.Write(buffer.Bytes())
		return errors.Wrap(err, "format failed")
	}

	if err := ioutil.WriteFile(root.OutputFilename, output, 0644); err != nil {
		return errors.Wrap(err, "write failed")
	}

	return nil
}

func resolveTypes(root *Root) error {
	domainTypes := make(map[string]string)

	for _, domain := range root.Domains {
		domainTypes[domain.Name] = domain.Type
	}

	resolveFields := func(context string, fields []*Field) error {
		for _, field := range fields {
			if field.Type != "" {
				continue
			}

			if typ, ok := domainTypes[field.Domain]; ok {
				field.Type = typ
			} else {
				return errors.Errorf("field %s.%s - domain %s not found", context, field.Name, field.Domain)
			}
		}
		return nil
	}

	for _, class := range root.Classes {
		if err := resolveFields(class.Name, class.Fields); err != nil {
			return err
		}

		for _, method := range class.Methods {
			if err := resolveFields(class.Name+"."+method.Name, method.Fields); err != nil {
				return err
			}
		}
	}

	return nil
}
