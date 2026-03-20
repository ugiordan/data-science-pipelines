/*
Copyright 2025.
Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at
    http://www.apache.org/licenses/LICENSE-2.0
Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package webhook

import (
	"fmt"
	"strings"

	"github.com/kubeflow/pipelines/api/v2alpha1/go/pipelinespec"
	"github.com/kubeflow/pipelines/backend/src/apiserver/template"
	"google.golang.org/protobuf/encoding/protojson"
)

// PipeClearConfig defines configurable validation rules.
type PipeClearConfig struct {
	BlockMutableTags    bool
	AllowedRegistries   []string
	MaxTasksPerPipeline int
}

// DefaultPipeClearConfig returns sensible defaults.
func DefaultPipeClearConfig() *PipeClearConfig {
	return &PipeClearConfig{
		BlockMutableTags:    true,
		AllowedRegistries:   nil, // nil = all allowed
		MaxTasksPerPipeline: 100,
	}
}

// PipeClearResult holds validation findings.
type PipeClearResult struct {
	Warnings []string
	Denials  []string
}

// ValidatePipelineSpec runs PipeClear validation on a parsed V2 pipeline template.
func ValidatePipelineSpec(tmpl *template.V2Spec, config *PipeClearConfig) (*PipeClearResult, error) {
	if config == nil {
		config = DefaultPipeClearConfig()
	}

	result := &PipeClearResult{}

	spec := tmpl.PipelineSpec()
	if spec == nil {
		return result, nil
	}

	// Parse deployment spec to get typed executors
	deploymentSpecStruct := spec.GetDeploymentSpec()
	if deploymentSpecStruct == nil {
		return result, nil
	}

	// Marshal structpb.Struct to JSON, then unmarshal to PipelineDeploymentConfig
	deploymentJSON, err := protojson.Marshal(deploymentSpecStruct)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal deployment spec: %w", err)
	}

	var deploymentConfig pipelinespec.PipelineDeploymentConfig
	if err := protojson.Unmarshal(deploymentJSON, &deploymentConfig); err != nil {
		return nil, fmt.Errorf("failed to parse deployment config: %w", err)
	}

	executors := deploymentConfig.GetExecutors()

	// Check max tasks
	if config.MaxTasksPerPipeline > 0 && len(executors) > config.MaxTasksPerPipeline {
		result.Denials = append(result.Denials,
			fmt.Sprintf("pipeline has %d tasks, exceeding maximum of %d", len(executors), config.MaxTasksPerPipeline))
	}

	// Validate each executor's container image
	for name, executor := range executors {
		containerSpec := executor.GetContainer()
		if containerSpec == nil {
			continue
		}

		image := containerSpec.GetImage()
		if image == "" {
			result.Denials = append(result.Denials,
				fmt.Sprintf("executor %q has no container image specified", name))
			continue
		}

		// Check mutable tags
		if config.BlockMutableTags {
			if !strings.Contains(image, ":") || strings.HasSuffix(image, ":latest") {
				result.Warnings = append(result.Warnings,
					fmt.Sprintf("image %q uses mutable tag - consider using a specific version", image))
			}
		}

		// Check allowed registries
		if len(config.AllowedRegistries) > 0 {
			registry := strings.SplitN(image, "/", 2)[0]
			allowed := false
			for _, r := range config.AllowedRegistries {
				if registry == r {
					allowed = true
					break
				}
			}
			if !allowed {
				result.Denials = append(result.Denials,
					fmt.Sprintf("image %q uses registry %q which is not in allowed list: %v", image, registry, config.AllowedRegistries))
			}
		}
	}

	return result, nil
}
