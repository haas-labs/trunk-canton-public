{{- define "trunk-canton.name" -}}
{{- default .Chart.Name .Values.nameOverride | trunc 63 | trimSuffix "-" -}}
{{- end -}}

{{- define "trunk-canton.fullname" -}}
{{- printf "%s-%s" .Release.Name (include "trunk-canton.name" .) | trunc 63 | trimSuffix "-" -}}
{{- end -}}

{{- define "trunk-canton.labels" -}}
app.kubernetes.io/name: {{ include "trunk-canton.name" . }}
app.kubernetes.io/instance: {{ .Release.Name }}
app.kubernetes.io/managed-by: {{ .Release.Service }}
helm.sh/chart: {{ .Chart.Name }}-{{ .Chart.Version }}
{{- end -}}

{{- define "trunk-canton.selectorLabels" -}}
app.kubernetes.io/name: {{ include "trunk-canton.name" . }}
app.kubernetes.io/instance: {{ .Release.Name }}
{{- end -}}
