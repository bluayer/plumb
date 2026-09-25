{{- define "plumb.name" -}}plumb{{- end -}}
{{- define "plumb.labels" -}}
app.kubernetes.io/part-of: plumb
app.kubernetes.io/managed-by: {{ .Release.Service }}
{{- end -}}

{{/* Enabled model instances as the decision service's models.json. */}}
{{- define "plumb.modelsConfig" -}}
{{- $models := dict -}}
{{- range $name, $m := .Values.decisionService.models -}}
{{- if $m.enabled -}}
{{- $_ := set $models $name (dict "plugin" $m.plugin "options" ($m.options | default dict) "temperatures" ($m.temperatures | default dict)) -}}
{{- end -}}
{{- end -}}
{{- if not (hasKey $models .Values.decisionService.default) -}}
{{- fail (printf "decisionService.default %q is not an enabled model" .Values.decisionService.default) -}}
{{- end -}}
{{- dict "default" .Values.decisionService.default "models" $models | toPrettyJson -}}
{{- end -}}

{{/* --decision-models value: name=timeout for each enabled instance. */}}
{{- define "plumb.shadowSpecs" -}}
{{- $specs := list -}}
{{- range $name, $m := .Values.decisionService.models -}}
{{- if $m.enabled -}}
{{- $specs = append $specs (printf "%s=%s" $name ($m.timeout | default "500ms")) -}}
{{- end -}}
{{- end -}}
{{- join "," $specs -}}
{{- end -}}
