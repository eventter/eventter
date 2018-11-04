package client

import (
	"regexp"

	"github.com/pkg/errors"
)

var (
	nameRegex       = regexp.MustCompile("^[a-zA-Z][0-9a-zA-Z-_]*$")
	validTopicTypes = map[string]bool{
		"direct":  true,
		"fanout":  true,
		"topic":   true,
		"headers": true,
	}
)

const (
	blankErrorFormat        = "%s cannot be blank"
	nameInvalidErrorFormat  = "%s must begin with letter and contain only letters, numbers, hyphens & underscores"
	stringLengthErrorFormat = "%s must be at most %d characters long"
	listErrorFormat         = "%s %s is not valid"
	negativeErrorFormat     = "%s must not be negative"
	positiveErrorFormat     = "%s must be positive"
	nameMaxLength           = 64
)

type RequestValidationError struct {
	Errors []error
}

func (e *RequestValidationError) Error() string {
	s := "There are following validation errors:\n"
	for _, err := range e.Errors {
		s += "  - " + err.Error() + "\n"
	}
	return s
}

func (r *ConfigureTopicRequest) Validate() error {
	var errs []error

	if r.Topic.Namespace == "" {
		errs = append(errs, errors.Errorf(blankErrorFormat, "namespace"))
	} else if !nameRegex.MatchString(r.Topic.Namespace) {
		errs = append(errs, errors.Errorf(nameInvalidErrorFormat, "namespace"))
	} else if len(r.Topic.Namespace) > nameMaxLength {
		errs = append(errs, errors.Errorf(stringLengthErrorFormat, "namespace", nameMaxLength))
	}

	if r.Topic.Name == "" {
		errs = append(errs, errors.Errorf(blankErrorFormat, "topic name"))
	} else if !nameRegex.MatchString(r.Topic.Name) {
		errs = append(errs, errors.Errorf(nameInvalidErrorFormat, "topic name"))
	} else if len(r.Topic.Name) > nameMaxLength {
		errs = append(errs, errors.Errorf(stringLengthErrorFormat, "topic name", nameMaxLength))
	}

	if r.Type == "" {
		errs = append(errs, errors.Errorf(blankErrorFormat, "topic type"))
	} else if !validTopicTypes[r.Type] {
		errs = append(errs, errors.Errorf(listErrorFormat, "topic type", r.Type))
	}

	if r.Retention < 0 {
		errs = append(errs, errors.Errorf(negativeErrorFormat, "retention"))
	}

	if len(errs) > 0 {
		return &RequestValidationError{errs}
	}

	return nil
}

func (r *ListTopicsRequest) Validate() error {
	var errs []error

	if r.Topic.Namespace == "" {
		errs = append(errs, errors.Errorf(blankErrorFormat, "namespace"))
	} else if !nameRegex.MatchString(r.Topic.Namespace) {
		errs = append(errs, errors.Errorf(nameInvalidErrorFormat, "namespace"))
	} else if len(r.Topic.Namespace) > nameMaxLength {
		errs = append(errs, errors.Errorf(stringLengthErrorFormat, "namespace", nameMaxLength))
	}

	if r.Topic.Name != "" {
		if !nameRegex.MatchString(r.Topic.Name) {
			errs = append(errs, errors.Errorf(nameInvalidErrorFormat, "topic name"))
		} else if len(r.Topic.Name) > nameMaxLength {
			errs = append(errs, errors.Errorf(stringLengthErrorFormat, "topic name", nameMaxLength))
		}
	}

	if len(errs) > 0 {
		return &RequestValidationError{errs}
	}

	return nil
}

func (r *DeleteTopicRequest) Validate() error {
	var errs []error

	if r.Topic.Namespace == "" {
		errs = append(errs, errors.Errorf(blankErrorFormat, "namespace"))
	} else if !nameRegex.MatchString(r.Topic.Namespace) {
		errs = append(errs, errors.Errorf(nameInvalidErrorFormat, "namespace"))
	} else if len(r.Topic.Namespace) > nameMaxLength {
		errs = append(errs, errors.Errorf(stringLengthErrorFormat, "namespace", nameMaxLength))
	}

	if r.Topic.Name == "" {
		errs = append(errs, errors.Errorf(blankErrorFormat, "topic name"))
	} else if !nameRegex.MatchString(r.Topic.Name) {
		errs = append(errs, errors.Errorf(nameInvalidErrorFormat, "topic name"))
	} else if len(r.Topic.Name) > nameMaxLength {
		errs = append(errs, errors.Errorf(stringLengthErrorFormat, "topic name", nameMaxLength))
	}

	if len(errs) > 0 {
		return &RequestValidationError{errs}
	}

	return nil
}

func (r *ConfigureConsumerGroupRequest) Validate() error {
	var errs []error

	if r.ConsumerGroup.Namespace == "" {
		errs = append(errs, errors.Errorf(blankErrorFormat, "namespace"))
	} else if !nameRegex.MatchString(r.ConsumerGroup.Namespace) {
		errs = append(errs, errors.Errorf(nameInvalidErrorFormat, "namespace"))
	} else if len(r.ConsumerGroup.Namespace) > nameMaxLength {
		errs = append(errs, errors.Errorf(stringLengthErrorFormat, "namespace", nameMaxLength))
	}

	if r.ConsumerGroup.Name == "" {
		errs = append(errs, errors.Errorf(blankErrorFormat, "consumer group name"))
	} else if !nameRegex.MatchString(r.ConsumerGroup.Name) {
		errs = append(errs, errors.Errorf(nameInvalidErrorFormat, "consumer group name"))
	} else if len(r.ConsumerGroup.Name) > nameMaxLength {
		errs = append(errs, errors.Errorf(stringLengthErrorFormat, "consumer group name", nameMaxLength))
	}

	if r.Shards < 1 {
		errs = append(errs, errors.Errorf(positiveErrorFormat, "shards"))
	}

	if len(errs) > 0 {
		return &RequestValidationError{errs}
	}

	return nil
}

func (r *ListConsumerGroupsRequest) Validate() error {
	var errs []error

	if r.ConsumerGroup.Namespace == "" {
		errs = append(errs, errors.Errorf(blankErrorFormat, "namespace"))
	} else if !nameRegex.MatchString(r.ConsumerGroup.Namespace) {
		errs = append(errs, errors.Errorf(nameInvalidErrorFormat, "namespace"))
	} else if len(r.ConsumerGroup.Namespace) > nameMaxLength {
		errs = append(errs, errors.Errorf(stringLengthErrorFormat, "namespace", nameMaxLength))
	}

	if r.ConsumerGroup.Name != "" {
		if !nameRegex.MatchString(r.ConsumerGroup.Name) {
			errs = append(errs, errors.Errorf(nameInvalidErrorFormat, "consumer group name"))
		} else if len(r.ConsumerGroup.Name) > nameMaxLength {
			errs = append(errs, errors.Errorf(stringLengthErrorFormat, "consumer group name", nameMaxLength))
		}
	}

	if len(errs) > 0 {
		return &RequestValidationError{errs}
	}

	return nil
}

func (r *DeleteConsumerGroupRequest) Validate() error {
	var errs []error

	if r.ConsumerGroup.Namespace == "" {
		errs = append(errs, errors.Errorf(blankErrorFormat, "namespace"))
	} else if !nameRegex.MatchString(r.ConsumerGroup.Namespace) {
		errs = append(errs, errors.Errorf(nameInvalidErrorFormat, "namespace"))
	} else if len(r.ConsumerGroup.Namespace) > nameMaxLength {
		errs = append(errs, errors.Errorf(stringLengthErrorFormat, "namespace", nameMaxLength))
	}

	if r.ConsumerGroup.Name == "" {
		errs = append(errs, errors.Errorf(blankErrorFormat, "consumer group name"))
	} else if !nameRegex.MatchString(r.ConsumerGroup.Name) {
		errs = append(errs, errors.Errorf(nameInvalidErrorFormat, "consumer group name"))
	} else if len(r.ConsumerGroup.Name) > nameMaxLength {
		errs = append(errs, errors.Errorf(stringLengthErrorFormat, "consumer group name", nameMaxLength))
	}

	if len(errs) > 0 {
		return &RequestValidationError{errs}
	}

	return nil
}
