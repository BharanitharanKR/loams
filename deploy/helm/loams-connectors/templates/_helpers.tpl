{{/* The release's object name prefix. */}}
{{- define "loams-connectors.fullname" -}}
{{- if contains .Chart.Name .Release.Name -}}
{{- .Release.Name | trunc 50 | trimSuffix "-" -}}
{{- else -}}
{{- printf "%s-%s" .Release.Name .Chart.Name | trunc 50 | trimSuffix "-" -}}
{{- end -}}
{{- end -}}

{{/* Labels every object of the chart carries. */}}
{{- define "loams-connectors.labels" -}}
app.kubernetes.io/name: {{ .Chart.Name }}
app.kubernetes.io/instance: {{ .Release.Name }}
app.kubernetes.io/version: {{ .Chart.AppVersion | quote }}
app.kubernetes.io/managed-by: {{ .Release.Service }}
helm.sh/chart: {{ printf "%s-%s" .Chart.Name .Chart.Version }}
{{- end -}}

{{/* The flow pods' selector. */}}
{{- define "loams-connectors.flowSelector" -}}
app.kubernetes.io/name: {{ .Chart.Name }}
app.kubernetes.io/instance: {{ .Release.Name }}
app.kubernetes.io/component: flow
{{- end -}}

{{/* The Kubernetes namespace of each served Loams namespace, one per line. */}}
{{- define "loams-connectors.servedK8sNamespaces" -}}
{{- range .Values.servedNamespaces }}
{{ replace "{ns}" (toString (int64 .)) $.Values.namespaceTemplate }}
{{- end -}}
{{- end -}}

{{- define "loams-connectors.fallbackServiceAccount" -}}
{{- default (printf "%s-fallback" (include "loams-connectors.fullname" .)) .Values.fallback.serviceAccount -}}
{{- end -}}

{{- define "loams-connectors.flowImage" -}}
{{- $i := .Values.image.flow -}}
{{- printf "%s@%s" $i.repository (required "image.flow.digest: pin the flow image by digest (sha256:...)" $i.digest) -}}
{{- end -}}

{{/*
The fleet configuration `loams-fabric flow` reads: KubeRuntime's FleetConfig
(fabric/crates/loams-flow/src/runtime/kube/mod.rs), JSON-valued YAML.
*/}}
{{- define "loams-connectors.fallbackRuntime" -}}
{{- $r := .r -}}
image:
  repository: {{ $r.image.repository | toJson }}
  digest: {{ required (printf "fallback.%s.image.digest: pin the image by digest (sha256:...)" .name) $r.image.digest | toJson }}
uid: {{ $r.uid | int64 }}
metrics_port: {{ $r.metricsPort | int64 }}
metrics_path: {{ $r.metricsPath | toJson }}
resources:
  cpu_request: {{ $r.resources.cpuRequest | toJson }}
  memory_request: {{ $r.resources.memoryRequest | toJson }}
  memory_limit: {{ $r.resources.memoryLimit | toJson }}
{{- with $r.storage }}
storage:
  size: {{ .size | toJson }}
  {{- with .storageClass }}
  storage_class: {{ . | toJson }}
  {{- end }}
{{- end }}
{{- end -}}

{{- define "loams-connectors.fleet" -}}
flow:
  k8s_namespace: {{ .Release.Namespace | toJson }}
  deployment: {{ printf "%s-flow" (include "loams-connectors.fullname" .) | toJson }}
  min_replicas: {{ .Values.flow.replicas.min | int64 }}
  max_replicas: {{ .Values.flow.replicas.max | int64 }}
  pod_memory_bytes: {{ .Values.flow.podBudget.memoryBytes | int64 }}
  max_instances_per_pod: {{ .Values.flow.podBudget.maxInstances | int64 }}
namespace_template: {{ .Values.namespaceTemplate | toJson }}
served_namespaces: [{{ range $i, $n := .Values.servedNamespaces }}{{ if $i }}, {{ end }}{{ $n | int64 }}{{ end }}]
cluster:
  pod_cidrs: {{ .Values.cluster.podCidrs | toJson }}
  service_cidrs: {{ .Values.cluster.serviceCidrs | toJson }}
  control_plane: {{ .Values.cluster.controlPlane.cidrs | toJson }}
  control_plane_hosts: {{ .Values.cluster.controlPlane.hosts | toJson }}
fabric:
  k8s_namespace: {{ default .Release.Namespace .Values.fabric.namespace | toJson }}
  pod_labels: {{ .Values.fabric.podLabels | toJson }}
  ports: [{{ range $i, $p := .Values.fabric.ports }}{{ if $i }}, {{ end }}{{ $p | int64 }}{{ end }}]
fallback:
  service_account: {{ include "loams-connectors.fallbackServiceAccount" . | toJson }}
  secret_mount_root: {{ .Values.fallback.secretMountRoot | toJson }}
  camel:
    {{- include "loams-connectors.fallbackRuntime" (dict "r" .Values.fallback.camel "name" "camel") | nindent 4 }}
  debezium:
    {{- include "loams-connectors.fallbackRuntime" (dict "r" .Values.fallback.debezium "name" "debezium") | nindent 4 }}
  iggy:
    {{- include "loams-connectors.fallbackRuntime" (dict "r" .Values.fallback.iggy "name" "iggy") | nindent 4 }}
{{- end -}}
