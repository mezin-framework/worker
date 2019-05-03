import traceback
import json
from redis import Redis
from threading import Thread
from hashlib import sha256
from datetime import datetime
from random import randint
from utils.work_distributer.requester import RefreshRequester
from utils.services import plugin_service

class RefreshWorker(object):
    ''' Responsible for using a Academic Parser
        to fetch information from the portal
        and communicate to API.
     '''

    def __init__(self):
        self.refresh_queue = RefreshQueue()
        self.working = False
        Watcher().start()

    def run(self):
        while True:
            try:
                data = self.refresh_queue.get_new_payload()
                self.working = True
                action = data.get('action')
                if action == 'install_plugin':
                    plugin_service.install_plugin(data.get('plugin_repo'))
                    self.refresh_queue.respond({'status': 'success'})
                else:
                    plugin = data.get('plugin')
                    print plugin, action
                    action = plugin_service.get_worker_action_from_plugin(plugin, action)
                    fetched_data = action(**data)
                    if not fetched_data:
                        fetched_data = {}
                    if not fetched_data.get('status'):
                        fetched_data['status'] = 'success'
                    self.refresh_queue.respond(fetched_data)
                self.working = False
            except:
                traceback.print_exc()
                self.refresh_queue.respond({"status": "internal_error"})
                self.working = False
                continue

class RefreshQueue(object):
    ''' Responsible for encapsulating Queue behaviour,
        such as getting a new payload.
    '''

    QUEUE = 'worker_queue'

    def __init__(self):
        self.redis = Redis(host='redis')
        self.work_id = ''

    def get_new_payload(self):
        key, value = self.redis.brpop(self.QUEUE)
        data = json.loads(value.decode())
        self.work_id = data.get('work_id')
        self.redis.lpush(self.work_id, json.dumps({"status": "processing"}))
        return data

    def respond(self, data):
        if self.work_id:
            self.redis.lpush(self.work_id, json.dumps(data))
            self.work_id = ''


class Watcher(Thread):

    def __init__(self):
        Thread.__init__(self)
        self.name = self.get_random_name()
        self.refresh_queue = RefreshQueue()
        self.refresh_queue.QUEUE = self.name
        self.requester = RefreshRequester('worker_registry')
        self.requester.block_request({"name": self.name, "action": "register"})

    def get_random_name(self):
        h = sha256()
        h.update(str(datetime.now()).encode('utf-8'))
        h.update(str(randint(0, 1000)))
        return h.hexdigest()

    def run(self):
        while True:
            try:
                data = self.refresh_queue.get_new_payload()
                action = data.get('action')
                if action == 'install_plugin':
                    plugin_service.install_plugin(data.get('plugin_repo'))
                    self.refresh_queue.respond({'status': 'success'})
                elif action == 'ping':
                    self.refresh_queue.respond({"status": "ok"})
            except:
                traceback.print_exc()
                self.refresh_queue.respond({'status': 'error'})
