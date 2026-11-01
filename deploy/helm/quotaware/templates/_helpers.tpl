{{- define "quotaware.name" -}}
{{- .Chart.Name -}}
{{- end -}}

{{- define "quotaware.fullname" -}}
{{- if contains .Chart.Name .Release.Name -}}
{{- .Release.Name | trunc 63 | trimSuffix "-" -}}
{{- else -}}
{{- printf "%s-%s" .Release.Name .Chart.Name | trunc 63 | trimSuffix "-" -}}
{{- end -}}
{{- end -}}

{{- define "quotaware.labels" -}}
app.kubernetes.io/name: {{ include "quotaware.name" . }}
app.kubernetes.io/instance: {{ .Release.Name }}
app.kubernetes.io/version: {{ .Values.image.tag | default .Chart.AppVersion | quote }}
app.kubernetes.io/managed-by: {{ .Release.Service }}
helm.sh/chart: {{ printf "%s-%s" .Chart.Name .Chart.Version }}
{{- end -}}

{{- define "quotaware.selectorLabels" -}}
app.kubernetes.io/name: {{ include "quotaware.name" . }}
app.kubernetes.io/instance: {{ .Release.Name }}
{{- end -}}
