{{- define "qentra-agent.name" -}}
{{- default .Chart.Name .Values.nameOverride | trunc 63 | trimSuffix "-" -}}
{{- end -}}

{{- define "qentra-agent.fullname" -}}
{{- printf "%s" (include "qentra-agent.name" .) | trunc 63 | trimSuffix "-" -}}
{{- end -}}

{{- define "qentra-agent.labels" -}}
app.kubernetes.io/name: {{ include "qentra-agent.name" . }}
app.kubernetes.io/instance: {{ .Release.Name }}
app.kubernetes.io/managed-by: {{ .Release.Service }}
app.kubernetes.io/version: {{ .Chart.AppVersion | quote }}
{{- end -}}
