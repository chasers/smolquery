{{- define "smolquery.validatePortSet" -}}
{{- $scope := .scope -}}
{{- $seen := dict -}}
{{- range $listener := .listeners -}}
{{- $port := int $listener.port -}}
{{- $key := printf "%d" $port -}}
{{- if hasKey $seen $key -}}
{{- fail (printf "%s ports %s and %s cannot both use port %d" $scope (get $seen $key) $listener.name $port) -}}
{{- end -}}
{{- $_ := set $seen $key $listener.name -}}
{{- end -}}
{{- end -}}

{{- define "smolquery.validateTopologyUpgrade" -}}
{{- if .Release.IsUpgrade -}}
{{- $root := . -}}
{{- $items := list -}}
{{- with (lookup "apps/v1" "StatefulSet" .Release.Namespace "") -}}
{{- $items = .items | default (list) -}}
{{- end -}}
{{- range $statefulSet := $items -}}
{{- $annotations := $statefulSet.metadata.annotations | default (dict) -}}
{{- $owned := and (eq (get $annotations "meta.helm.sh/release-name") $root.Release.Name) (eq (get $annotations "meta.helm.sh/release-namespace") $root.Release.Namespace) -}}
{{- if $owned -}}
{{- $labels := $statefulSet.metadata.labels | default (dict) -}}
{{- $component := get $labels "app.kubernetes.io/component" -}}
{{- if and (eq $root.Values.topology "split") (eq $component "server") -}}
{{- fail "topology cannot change from symmetric to split in place; install a fresh release and migrate" -}}
{{- end -}}
{{- if and (eq $root.Values.topology "symmetric") (has $component (list "api" "buffer" "storage")) -}}
{{- fail "topology cannot change from split to symmetric in place; install a fresh release and migrate" -}}
{{- end -}}
{{- if has $component (list "api" "buffer" "storage" "server") -}}
{{- $expectedName := include "smolquery.roleName" (dict "root" $root "role" $component) -}}
{{- if ne $statefulSet.metadata.name $expectedName -}}
{{- fail "nameOverride and fullnameOverride cannot change for an existing release because StatefulSet and PVC identities are stable" -}}
{{- end -}}
{{- end -}}
{{- end -}}
{{- end -}}
{{- end -}}
{{- end -}}

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
{{- $ports := .Values.service.ports -}}
{{- $epmd := dict "name" "epmd" "port" 4369 -}}
{{- $metrics := dict "name" "metrics" "port" $ports.metrics -}}
{{- $genRpc := dict "name" "genRpc" "port" $ports.genRpc -}}
{{- $genRpcTls := dict "name" "genRpcTls" "port" $ports.genRpcTls -}}
{{- $http := dict "name" "http" "port" $ports.http -}}
{{- $web := dict "name" "web" "port" $ports.web -}}
{{- $hotServer := dict "name" "hotServer" "port" $ports.hotServer -}}
{{- include "smolquery.validatePortSet" (dict "scope" "headless Service" "listeners" (list $epmd $genRpc $genRpcTls $hotServer $metrics)) -}}
{{- include "smolquery.validatePortSet" (dict "scope" "front-door Service" "listeners" (list $http $web)) -}}
{{- $transport := ternary $genRpcTls $genRpc .Values.tls.enabled -}}
{{- $common := list $epmd $metrics $transport -}}
{{- if eq .Values.topology "split" -}}
{{- include "smolquery.validatePortSet" (dict "scope" "api pod" "listeners" (concat $common (list $http $web))) -}}
{{- include "smolquery.validatePortSet" (dict "scope" "buffer pod" "listeners" (concat $common (list $hotServer))) -}}
{{- include "smolquery.validatePortSet" (dict "scope" "storage pod" "listeners" $common) -}}
{{- else -}}
{{- include "smolquery.validatePortSet" (dict "scope" "server pod" "listeners" (concat $common (list $http $web $hotServer))) -}}
{{- end -}}
{{- include "smolquery.validateTopologyUpgrade" . -}}
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
