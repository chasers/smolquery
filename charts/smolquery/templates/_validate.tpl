{{- define "smolquery.validate" -}}
{{- $reservedEnvKeys := list "SMOLQUERY_API_PORT" "SMOLQUERY_WEB_PORT" "SMOLQUERY_HOT_SERVER_PORT" "SMOLQUERY_METRICS_PORT" "GEN_RPC_PORT" "GEN_RPC_SSL_PORT" "HEADLESS_SERVICE" "SMOLQUERY_BUFFER_REPLICATION" "SMOLQUERY_BUFFER_REPLICAS" "SMOLQUERY_BUFFER_STATEFULSET" "GEN_RPC_TLS" "DIST_TLS" "SMOLQUERY_ROLES" "POD_NAME" "POD_NAMESPACE" -}}
{{- range $key, $_ := .Values.env }}
{{- if has $key $reservedEnvKeys }}
{{- fail (printf "env.%s is reserved and configured by the chart" $key) }}
{{- end }}
{{- end }}
{{- range $entry := .Values.commonExtraEnv }}
{{- if has $entry.name $reservedEnvKeys }}
{{- fail (printf "commonExtraEnv name %s is reserved and configured by the chart" $entry.name) }}
{{- end }}
{{- end }}
{{- range $role, $entries := .Values.roleExtraEnv }}
{{- range $entry := $entries }}
{{- if has $entry.name $reservedEnvKeys }}
{{- fail (printf "roleExtraEnv.%s name %s is reserved and configured by the chart" $role $entry.name) }}
{{- end }}
{{- end }}
{{- end }}
{{- if not .Values.existingSecret.name -}}
{{- fail "existingSecret.name is required and must name an existing Secret" -}}
{{- end -}}
{{- if and (eq .Values.topology "split") (gt (int .Values.replicationFactor) (int .Values.replicas.buffer)) -}}
{{- fail (printf "replicationFactor (%v) cannot exceed split buffer replicas (%v)" .Values.replicationFactor .Values.replicas.buffer) -}}
{{- end -}}
{{- if and (eq .Values.topology "symmetric") (gt (int .Values.replicationFactor) (int .Values.replicas.server)) -}}
{{- fail (printf "replicationFactor (%v) cannot exceed symmetric server replicas (%v)" .Values.replicationFactor .Values.replicas.server) -}}
{{- end -}}
{{- if and .Values.tls.enabled (not .Values.tls.secretName) -}}
{{- fail "tls.secretName is required when tls.enabled is true" -}}
{{- end -}}
{{- if and .Values.podOperations.enabled (not .Values.serviceAccount.create) (not .Values.serviceAccount.name) -}}
{{- fail "podOperations requires serviceAccount.create=true or serviceAccount.name" -}}
{{- end -}}
{{- end -}}
