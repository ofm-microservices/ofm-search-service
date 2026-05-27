package config

import "fmt"

// WrapParseEnvConfigError annotates environment parse failures.
func WrapParseEnvConfigError(err error) error {
	return fmt.Errorf("parse env config: %w", err)
}
