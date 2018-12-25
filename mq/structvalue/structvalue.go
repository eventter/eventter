package structvalue

import (
	"strconv"
	"time"

	"github.com/gogo/protobuf/types"
	"github.com/pkg/errors"
)

func Uint32(s *types.Struct, field string, defaultValue uint32) (uint32, error) {
	if s == nil || s.Fields == nil {
		return defaultValue, nil
	}
	value, ok := s.Fields[field]
	if !ok {
		return defaultValue, nil
	}

	switch value := value.Kind.(type) {
	case *types.Value_NumberValue:
		return uint32(value.NumberValue), nil
	case *types.Value_StringValue:
		i, err := strconv.ParseUint(value.StringValue, 10, 32)
		if err != nil {
			return defaultValue, errors.Wrap(err, "parse number failed")
		}
		return uint32(i), nil
	default:
		return defaultValue, errors.Errorf("unexpected value kind %T", value)
	}
}

func Duration(s *types.Struct, field string, defaultValue time.Duration) (time.Duration, error) {
	if s == nil || s.Fields == nil {
		return defaultValue, nil
	}
	value, ok := s.Fields[field]
	if !ok {
		return defaultValue, nil
	}

	switch value := value.Kind.(type) {
	case *types.Value_NumberValue:
		return time.Duration(value.NumberValue), nil
	case *types.Value_StringValue:
		d, err := time.ParseDuration(value.StringValue)
		if err != nil {
			return defaultValue, errors.Wrap(err, "parse duration failed")
		}
		return d, nil
	default:
		return defaultValue, errors.Errorf("unexpected value kind %T", value)
	}
}
