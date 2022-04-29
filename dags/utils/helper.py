def apply_task_downstream(sequence, extra_step=None):
    if extra_step:
        sequence.append(extra_step)

    if len(sequence) > 1:
        for idx, curr in enumerate(sequence[1:], start=1):
            prev = sequence[idx - 1]
            prev >> curr
