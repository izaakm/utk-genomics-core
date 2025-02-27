from simple_slurm import Slurm

default_kwargs = dict(
    job_name='gensvc-convert',
    account='ISAAC-UTK0192',
    nodes=1,
    ntasks_per_node=48,
    exclusive='mcs',
    partition='short',
    qos='short',
    time='0-03:00:00',
    output='job-%J-%x.o'
)
