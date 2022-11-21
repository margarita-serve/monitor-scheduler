import time
from apscheduler.schedulers.blocking import BlockingScheduler

import action

sched = BlockingScheduler()


@sched.scheduled_job('interval', seconds=30, id='drift')
def drift():
    action.drift_monitor()


@sched.scheduled_job('interval', seconds=30, id='accuracy')
def accuracy():
    action.accuracy_monitor()


@sched.scheduled_job('interval', seconds=60, id='service')
def service():
    action.service_monitor()


sched.start()
