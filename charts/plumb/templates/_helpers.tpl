{{- define "plumb.name" -}}plumb{{- end -}}
{{- define "plumb.labels" -}}
app.kubernetes.io/part-of: plumb
app.kubernetes.io/managed-by: {{ .Release.Service }}
{{- end -}}
{{- define "plumb.image" -}}{{ .Values.image.repository }}:{{ .Values.image.tag | default .Chart.AppVersion }}{{- end -}}
{{- define "plumb.podSecurityContext" -}}
securityContext: {runAsNonRoot: true, seccompProfile: {type: RuntimeDefault}}
{{- end -}}
{{/* Scheduling and pull settings shared by both Deployments; takes (list $ .Values.<component> "<component>").
Without an affinity, the replicas prefer different nodes (replicas on one node fail together)
and still schedule on a single node. */}}
{{- define "plumb.podScheduling" -}}
{{- $root := index . 0 }}{{ $c := index . 1 }}{{ $name := index . 2 -}}
{{- with $root.Values.imagePullSecrets }}
imagePullSecrets: {{- toYaml . | nindent 2 }}
{{- end }}
{{- with $c.priorityClassName }}
priorityClassName: {{ . }}
{{- end }}
{{- with $c.nodeSelector }}
nodeSelector: {{- toYaml . | nindent 2 }}
{{- end }}
{{- with $c.tolerations }}
tolerations: {{- toYaml . | nindent 2 }}
{{- end }}
{{- if $c.affinity }}
affinity: {{- toYaml $c.affinity | nindent 2 }}
{{- else }}
affinity:
  podAntiAffinity:
    preferredDuringSchedulingIgnoredDuringExecution:
      - weight: 100
        podAffinityTerm:
          topologyKey: kubernetes.io/hostname
          labelSelector:
            matchLabels: {app.kubernetes.io/name: {{ include "plumb.name" $root }}-{{ $name }}}
{{- end }}
{{- end -}}
