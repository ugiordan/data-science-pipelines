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
	"testing"

	"github.com/kubeflow/pipelines/backend/src/apiserver/template"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func newTestTemplate(t *testing.T, specJSON string) *template.V2Spec {
	t.Helper()
	tmpl, err := template.NewV2SpecTemplate([]byte(specJSON), template.TemplateOptions{})
	require.NoError(t, err, "failed to create V2Spec template")
	return tmpl
}

const baseSpecTemplate = `{
	"pipelineInfo": {"name": "test-pipeline"},
	"root": {"dag": {"tasks": {}}},
	"schemaVersion": "2.1.0",
	"sdkVersion": "kfp-2.11.0",
	"deploymentSpec": {
		"executors": {
			%s
		}
	}
}`

func specWithExecutors(executors string) string {
	return fmt.Sprintf(baseSpecTemplate, executors)
}

func TestPipeClear_AllowCleanPipeline(t *testing.T) {
	spec := specWithExecutors(`"exec-train": {
		"container": {
			"image": "registry.redhat.io/ubi9/python-311:1.0",
			"command": ["python"],
			"args": ["train.py"]
		}
	}`)
	tmpl := newTestTemplate(t, spec)
	result, err := ValidatePipelineSpec(tmpl, DefaultPipeClearConfig())
	require.NoError(t, err)
	assert.Empty(t, result.Denials, "expected no denials for a clean pipeline")
	assert.Empty(t, result.Warnings, "expected no warnings for a tagged image")
}

func TestPipeClear_WarnMutableTag(t *testing.T) {
	spec := specWithExecutors(`"exec-train": {
		"container": {
			"image": "registry.redhat.io/ubi9/python-311:latest",
			"command": ["python"],
			"args": ["train.py"]
		}
	}`)
	tmpl := newTestTemplate(t, spec)
	result, err := ValidatePipelineSpec(tmpl, DefaultPipeClearConfig())
	require.NoError(t, err)
	assert.Empty(t, result.Denials, "expected no denials")
	assert.Len(t, result.Warnings, 1, "expected one warning for :latest tag")
	assert.Contains(t, result.Warnings[0], "mutable tag")
}

func TestPipeClear_DenyMissingImage(t *testing.T) {
	spec := specWithExecutors(`"exec-train": {
		"container": {
			"image": "",
			"command": ["python"]
		}
	}`)
	tmpl := newTestTemplate(t, spec)
	result, err := ValidatePipelineSpec(tmpl, DefaultPipeClearConfig())
	require.NoError(t, err)
	assert.Len(t, result.Denials, 1, "expected one denial for missing image")
	assert.Contains(t, result.Denials[0], "no container image specified")
}

func TestPipeClear_DenyTooManyTasks(t *testing.T) {
	spec := specWithExecutors(`"exec-train": {
		"container": {
			"image": "registry.redhat.io/ubi9/python-311:1.0",
			"command": ["python"]
		}
	},
	"exec-eval": {
		"container": {
			"image": "registry.redhat.io/ubi9/python-311:1.0",
			"command": ["python"]
		}
	}`)
	tmpl := newTestTemplate(t, spec)
	config := &PipeClearConfig{
		BlockMutableTags:    true,
		MaxTasksPerPipeline: 1,
	}
	result, err := ValidatePipelineSpec(tmpl, config)
	require.NoError(t, err)
	assert.Len(t, result.Denials, 1, "expected one denial for too many tasks")
	assert.Contains(t, result.Denials[0], "exceeding maximum")
}

func TestPipeClear_DenyDisallowedRegistry(t *testing.T) {
	spec := specWithExecutors(`"exec-train": {
		"container": {
			"image": "docker.io/library/python:3.11",
			"command": ["python"]
		}
	}`)
	tmpl := newTestTemplate(t, spec)
	config := &PipeClearConfig{
		BlockMutableTags:    false,
		AllowedRegistries:   []string{"registry.redhat.io", "quay.io"},
		MaxTasksPerPipeline: 100,
	}
	result, err := ValidatePipelineSpec(tmpl, config)
	require.NoError(t, err)
	assert.Len(t, result.Denials, 1, "expected one denial for disallowed registry")
	assert.Contains(t, result.Denials[0], "docker.io")
	assert.Contains(t, result.Denials[0], "not in allowed list")
}

func TestPipeClear_AllowAllRegistriesWhenNil(t *testing.T) {
	spec := specWithExecutors(`"exec-train": {
		"container": {
			"image": "docker.io/library/python:3.11",
			"command": ["python"]
		}
	}`)
	tmpl := newTestTemplate(t, spec)
	config := &PipeClearConfig{
		BlockMutableTags:    false,
		AllowedRegistries:   nil,
		MaxTasksPerPipeline: 100,
	}
	result, err := ValidatePipelineSpec(tmpl, config)
	require.NoError(t, err)
	assert.Empty(t, result.Denials, "expected no denials when AllowedRegistries is nil")
	assert.Empty(t, result.Warnings, "expected no warnings when BlockMutableTags is false")
}
