{{- define "smolquery.name" -}}
{{- default .Chart.Name .Values.nameOverride | trunc 63 | trimSuffix "-" -}}
{{- end -}}
{{- define "smolquery.fullname" -}}
{{- if .Values.fullnameOverride -}}
{{- .Values.fullnameOverride | trunc 47 | trimSuffix "-" -}}
{{- else -}}
{{- printf "%s-%s" .Release.Name (include "smolquery.name" .) | trunc 47 | trimSuffix "-" -}}
{{- end -}}
{{- end -}}
{{- define "smolquery.chart" -}}
{{- printf "%s-%s" .Chart.Name .Chart.Version | replace "+" "_" | trunc 63 | trimSuffix "-" -}}
{{- end -}}
{{- define "smolquery.labels" -}}
helm.sh/chart: {{ include "smolquery.chart" . }}
app.kubernetes.io/name: {{ include "smolquery.name" . }}
app.kubernetes.io/instance: {{ .Release.Name }}
app.kubernetes.io/version: {{ .Chart.AppVersion | quote }}
app.kubernetes.io/managed-by: {{ .Release.Service }}
{{- end -}}
{{- define "smolquery.selector" -}}
app.kubernetes.io/name: {{ include "smolquery.name" . }}
app.kubernetes.io/instance: {{ .Release.Name }}
{{- end -}}
{{- define "smolquery.headless" -}}{{ include "smolquery.fullname" . }}-headless{{- end -}}
{{- define "smolquery.image" -}}
{{- if .Values.image.digest -}}{{ .Values.image.repository }}@{{ .Values.image.digest }}{{- else -}}{{ .Values.image.repository }}:{{ .Values.image.tag }}{{- end -}}
{{- end -}}
{{- define "smolquery.serviceAccountName" -}}
{{- if .Values.serviceAccount.name -}}{{ .Values.serviceAccount.name }}{{- else -}}{{ include "smolquery.fullname" . }}{{- end -}}
{{- end -}}
{{- define "smolquery.checksum" -}}{{ include (print $.Template.BasePath "/configmap.yaml") . | sha256sum }}{{- end -}}
{{- define "smolquery.roleName" -}}
{{- printf "%s-%s" (include "smolquery.fullname" .root) .role | trunc 63 | trimSuffix "-" -}}
{{- end -}}
{{- define "smolquery.roleRoles" -}}
{{- if eq .role "api" -}}api,ingest,query,web{{ else if eq .role "buffer" -}}buffer{{ else if eq .role "storage" -}}storage{{ else -}}all{{ end -}}
{{- end -}}
{{- define "smolquery.roleReplicas" -}}
{{- if eq .role "api" -}}{{ .root.Values.replicas.api }}{{ else if eq .role "buffer" -}}{{ .root.Values.replicas.buffer }}{{ else if eq .role "storage" -}}{{ .root.Values.replicas.storage }}{{ else -}}{{ .root.Values.replicas.server }}{{ end -}}
{{- end -}}
{{- define "smolquery.rolePorts" -}}
{{- if or (eq .role "api") (eq .role "server") }}
            - name: http
              containerPort: {{ .root.Values.service.ports.http }}
            - name: web
              containerPort: {{ .root.Values.service.ports.web }}
{{- end }}
{{- if or (eq .role "buffer") (eq .role "server") }}
            - name: hot-server
              containerPort: {{ .root.Values.service.ports.hotServer }}
{{- end }}
            - name: metrics
              containerPort: {{ .root.Values.service.ports.metrics }}
            - name: epmd
              containerPort: 4369
            - name: gen-rpc
              containerPort: {{ .root.Values.service.ports.genRpc }}
            - name: gen-rpc-tls
              containerPort: {{ .root.Values.service.ports.genRpcTls }}
{{- end -}}
