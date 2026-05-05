brine_pillar_echo:
  file.managed:
    - name: /tmp/brine-pillar-echo
    - contents: {{ salt['pillar.get']('brine:message', 'missing') | json }}
