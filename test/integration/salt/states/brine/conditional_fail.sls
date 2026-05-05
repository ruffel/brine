{% if grains['id'] == 'minion-2' %}
brine_conditional_failure:
  test.fail_without_changes:
    - name: brine conditional failure on minion-2
{% else %}
brine_conditional_success:
  test.succeed_without_changes:
    - name: brine conditional success
{% endif %}
