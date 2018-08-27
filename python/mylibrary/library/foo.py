import json
import requests
import pika

STATES = {"NULL": "0", "WAITING": "1", "RUNNING": "2",
          "DONE": "3", "FAILED": "4", "STOPPED": "5",
          "CANCELLED": "6", "WAITING-BRANCH": "7"}


class Nasha:
    def __init__(self, host=None, port=None, endpoint=None):
        self.endpoint = None
        self.host = None
        self.port = None

        if not self.endpoint:
            if not (host and port):
                raise Exception('Invalid Nasha endpoint')
            else:
                self.host = host
                self.port = port
                self.endpoint = 'http://' + host + ":" + str(port) + "/api/v1/"
        else:
            if not endpoint:
                raise Exception('Invalid Nasha endpoint')
            else:
                self.endpoint = endpoint

    def get_ticket_by_id(self, ticket_id):
        '''
        Gets a ticket by the uuid registered in Nasha
        :param ticket_id: UUID of the ticket
        :return: A Ticket object if ticket was found, None otherwise
        '''

        # URL: get_ticket_by_id
        # Payload: {"ticket_id": "xxx"}

        data = {"ticket_id": ticket_id}
        payload = json.dumps(data)
        req = requests.post(self.endpoint + 'get_ticket_by_id', payload)

        if req.status_code != 200:
            return None

        print(req.text)

        json_data = json.loads(req.text)

        ticket = Ticket.from_json(json_data)

        return ticket

    def get_tickets_by_flow(self, flow_id):
        '''
        Gets all ticket of a specific flow type by the uuid registered in Nasha
        :param flow_id: UUID of the flow
        :return: A list of Ticket objects if found, None otherwise
        '''

        # URL: get_tickets_by_flow
        # Payload: {"flow_id": "xxx"}

        data = {"flow_id": flow_id}
        payload = json.dumps(data)
        req = requests.post(self.endpoint + 'get_tickets_by_flow', payload)

        if req.status_code != 200:
            return None

        print(req.text)

        json_data = json.loads(req.text)

        tickets = []
        for ticket in json_data:
            tickets.append(Ticket.from_json(ticket))

        return tickets

    def get_tickets_by_state(self, state):
        '''
        Gets all ticket in a specific state registered in Nasha
        :param state: UUID of the flow
        :return: A list of Ticket objects if found, None otherwise
        '''

        # URL: get_tickets_by_state
        # Payload: {"state": "DONE"}

        data = {"state": state}
        payload = json.dumps(data)
        req = requests.post(self.endpoint + 'get_tickets_by_state', payload)

        print(req.text)

        if req.status_code != 200:
            return None

        json_data = json.loads(req.text)

        tickets = []
        for ticket in json_data:
            tickets.append(Ticket.from_json(ticket))

        return tickets

    def get_all_tickets(self):
        '''
        Gets all tickets from Nasha
        :return: A list of Ticket objects if there are any, None otherwise
        '''

        # URL: get_all_tickets

        req = requests.get(self.endpoint + 'get_all_tickets')

        print(req.text)

        if req.status_code != 200:
            return None

        json_data = json.loads(req.text)

        tickets = []
        for ticket in json_data:
            tickets.append(Ticket.from_json(ticket))

        return tickets

    def get_flow_by_id(self, flow_id):
        '''
        Gets a flow by the uuid registered in Nasha
        :return: A Flow object if flow was found, None otherwise
        '''

        # URL: get_flow
        # Payload: {"flow_id": "xxx"}

        data = {"flow_id": flow_id}
        payload = json.dumps(data)

        req = requests.post(self.endpoint + 'get_flow', payload)
        if req.status_code != 200:
            return None

        print(req.text)

        json_data = json.loads(req.text)
        flow = Flow.from_flowdef(json_data)
        return flow

    def get_all_flows(self):
        '''
        Gets all flows registered in Nasha
        :return: A list of Flow object if there are any, None otherwise
        '''

        # URL: get_all_flows

        req = requests.get(self.endpoint + 'get_all_flows')
        if req.status_code != 200:
            return None

        print(req.text)

        json_data = json.loads(req.text)

        flows = []
        for flow in json_data:
            flows.append(Flow.from_flowdef(flow))

        return flows

    def register_flow(self, flowdef):
        '''
        Registers a flow in Nasha
        :param flowdef: A flowdef definition of a Nasha flow
        :return: FlowId from Nasha if successful, otherwise None
        '''

        # URL: /api/v1/register_flow
        # Payload: flowdef
        data = {"flow_definition": flowdef}
        payload = json.dumps(data)

        print(payload)

        req = requests.post(self.endpoint + 'register_flow', payload)
        if req.status_code != 200:
            return None

        print(req.text)

        json_data = json.loads(req.text)

        return json_data

    def create_ticket(self, flow_id, metadata={}):
        '''
        Create a ticket in Nasha
        :param flow_id: Id of flow that we want to create a ticket of
        :param metadata: Metadata for the ticket
        :return: The ticket Id for the created ticket
        '''

        # URL: create_ticket
        # Payload: {"flow_id": "xxx", "metadata": {}}

        data = {"flow_id": flow_id, "metadata": metadata}
        payload = json.dumps(data)

        print(payload)

        req = requests.post(self.endpoint + 'create_ticket', payload)
        if req.status_code != 200:
            return None

        print(req.text)

        json_data = json.loads(req.text)

        #return self.get_ticket_by_id(json_data['ticket_id'])
        return json_data['ticket_id']

    def start_ticket(self, ticket_id):
        '''
         Marks the ticket as started in Nasha
         :param ticket_id: Id of the we want to start
         :return: True if successful, False otherwise
         '''

        # URL: update_ticket
        # Payload: {"ticket_id": "xxx", "metadata": "xxx"}

        data = {"ticket_id": ticket_id}
        payload = json.dumps(data)

        req = requests.post(self.endpoint + 'start_ticket', payload)
        print(req.text)

        if req.status_code != 200:
            return False

        return True

    def update_ticket(self, ticket_id, metadata):
        '''
        Updates ticket metadata in Nasha
        :param ticket_id: Id of the ticket we want to update
        :param metadata: Metadata containing information we want to update on ticket
        :return: True if successful, False otherwise
        '''

        # URL: update_ticket
        # Payload: {"ticket_id": "xxx", "metadata": {}}

        data = {"ticket_id": ticket_id, "metadata": metadata}
        payload = json.dumps(data)
        print(payload)

        req = requests.post(self.endpoint + 'update_ticket', payload)
        print(req.text)

        if req.status_code != 200:
            return False

        return True

    def update_task(self, task_job):
        '''
        Updates a task belonging to a ticket in Nasha
        :param task_job: TaskJob object holding the information we want to update
        :return: True if successful, otherwise False
        '''

        # URL: update_ticket_task
        # Payload: '{"ticket_id": "1", "task_id": "1", "new_state": "DONE", "metadata": null}'

        metadata = task_job.task_data
        data = {"ticket_id": task_job.ticket_id, "task_id": task_job.task_id, "new_state": task_job.state,
                "metadata": metadata}
        payload = json.dumps(data)
        print(payload)

        req = requests.post(self.endpoint + 'update_ticket_task', payload)
        print(req.text)
        if req.status_code != 200:
            return False

        return True

    def create_processor(self, processor_id, metadata={}):
        '''
        Creates processor in Nasha
        :param processor_id: Id of processor to create in Nasha
        :param metadata: Metadata for processor
        :return: True if successful, False otherwise
        '''

        # URL: create_processor
        # Payload: {"processor_id": "xxx", "metadata": {}}

        data = {"processor_id": processor_id, "metadata": metadata}
        payload = json.dumps(data)

        req = requests.post(self.endpoint + 'create_processor', payload)
        print(req.text)

        if req.status_code != 200:
            return False

        return True

    def register_processor(self, processor_id, task_id):
        '''
        Register a processor for a task in Nasha
        :param processor_id: Id of the processor to register
        :param task_id: Id of the task to register the processor to
        :return: True if successful, False otherwise
        '''

        # URL: register_processor
        # Payload: {"processor_id": "xxx", "task_id": "xxx"}

        data = {"processor_id": processor_id, "task_id": task_id}
        payload = json.dumps(data)

        req = requests.post(self.endpoint + 'register_processor', payload)
        print(req.text)

        if req.status_code != 200:
            return False

        return True

    def unregister_processor(self, processor_id, task_id):
        '''
        Unregisters a processor for a task in Nasha
        :param processor_id: Id of a Processor to unregister
        :param task_id: Id of the task to unregister a processor from
        :return: True if succesful, otherwise False
        '''

        # URL: unregister_processor
        # Payload: {"processor_id": "xxx", "task_id": "xxx"}

        data = {"processor_id": processor_id, "task_id": task_id}
        payload = json.dumps(data)

        req = requests.post(self.endpoint + 'unregister_processor', payload)
        print(req.text)

        if req.status_code != 200:
            return False

        return True

    def delete_processor(self, processor_id):
        '''
        Deletes a given processor from Nasha
        :param processor_id: Id of a Processor to delete
        :return: True if successful, otherwise False
        '''

        # URL: delete_processor
        # Payload: {"processor_id": "xxx"}

        data = {"processor_id": processor_id}
        payload = json.dumps(data)

        req = requests.post(self.endpoint + 'delete_processor', payload)
        print(req.text)

        if req.status_code != 200:
            return False

        return True

    def get_processor(self, processor_id):
        '''
        Gets a processor from Nasha
        :param processor_id: UUID of the processor we want get
        :return: A processor object
        '''

        # URL: get_processor_entry
        # Payload: {"processor_id": "xxx"}

        data = {"processor_id": processor_id}
        payload = json.dumps(data)

        req = requests.post(self.endpoint + 'get_processor_entry', payload)
        print(req.text)

        if req.status_code != 200:
            return False

        json_data = json.loads(req.text)

        processor = Processor.from_json(json_data)

        return processor

    def get_all_processors(self):
        '''
        Get information about a processor from Nasha
        :return: A list of Processor objects
        '''

        # URL: get_all_processor_entry
        req = requests.get(self.endpoint + 'get_all_processor_entry')
        print(req.text)

        if req.status_code != 200:
            return False

        json_data = json.loads(req.text)
        processors = []
        for processor in json_data:
            processors.append(Processor.from_json(processor))

        return processors


class Task:
    def __init__(self, name=None, metadata={}, dependson=None, parenttask=None, is_start=False, uuid=None,
                 state=None, ticket=None):
        if not uuid:
            self.uuid = name
        else:
            self.uuid = uuid
        self.name = name
        self.metadata = metadata
        self.is_start = is_start
        self.dependson = dependson
        self.parenttask = parenttask
        self.childtasks = []
        self.next = []
        self.state = state
        self.ticket = ticket

        if dependson:
            dependson.next.append(self)

        if parenttask:
            parenttask.childtasks.append(self)

    @classmethod
    def from_json(clr, json_data):
        '''
        Generate a Task object from json
        :param json_data: Task object in json format
        :return: A Task object with information from json_data
        '''
        if isinstance(json_data, bytes):
            data = json_data.decode('utf-8')
            json_data = json.loads(data)

        task = clr(uuid=json_data['task_id'], state=json_data['state'], ticket=Ticket(uuid=json_data['ticket_id']))
        return task


class Flow:
    def __init__(self, name=None, metadata={}, uuid=None):
        if not uuid:
            self.uuid = name
        else:
            self.uuid = uuid
        self.name = name
        self.metadata = metadata
        self.tasks = []


    @classmethod
    def from_flowdef(cls, flowdef):
        '''
        Generate a flow object from flowdef
        :param flowdef: Definition of a flow in json format
        :return: A flow object created from flowdef
        '''
        flow = cls(flowdef['name'], str(flowdef['metadata']), flowdef['id'])

        for task in flowdef['tasks']:
            temp = Task(task['name'], task['metadata'])
            temp.uuid = task['id']
            temp.name = task['name']
            temp.is_start = task['is_start']
            flow.add_task(temp)

        if flowdef['transitions']:
            for trans in flowdef['transitions']:
                to_task = flow.get_task(trans['to_task'])
                from_task = flow.get_task(trans['from_task'])
                to_task.dependson = from_task
                from_task.next.append(to_task)

        if flowdef['dependencies']:
            for deps in flowdef['dependencies']:
                child = flow.get_task(deps['child_task'])
                parent = flow.get_task(deps['parent_task'])
                child.parenttask = parent
                parent.childtasks.append(child)

        return flow

    def to_flowdef(self):
        '''
        Converts Flow to flowdef format
        :return: Flowdef in json format
        '''

        # If task has no uuid, we need Nasha to generate one, so we use name as uuid instead

        tasks = []
        for task in self.tasks:
            temp = {"id": task.uuid, "name": task.name, "metadata": task.metadata, "is_start": task.is_start}
            tasks.append(temp)

        transitions = []
        dependencies = []

        for task in self.tasks:
            if task.dependson:
                temp = {"from_task": task.dependson.uuid, "to_task": task.uuid}
                transitions.append(temp)
            if task.parenttask:
                temp = {"parent_task": task.parenttask.uuid, "child_task": task.uuid}
                dependencies.append(temp)

        flowdef = {"id": self.uuid, "name": self.name, "metadata": self.metadata, "tasks": tasks,
                   "transitions": transitions, "dependencies": dependencies}

        return flowdef

    def add_task(self, task):
        '''
        Add a task to the flow
        :param task: The task to be added
        :return: True if successful, False otherwise
        '''
        for t in self.tasks:
            if t.name == task.name:
                return False

        if task.is_start is True:
            self.tasks.insert(0, task)
        else:
            self.tasks.append(task)

        return True

    def get_task(self, uuid):
        '''
        Get a task by UUID
        :param uuid: The UUID of the task we want to get
        :return: A task
        '''
        for task in self.tasks:
            if task.uuid == uuid:
                return task

    def get_task_by_name(self, task_name):
        '''
        Gets a specific task from the ticket
        :return: A Task object if successful, None otherwise
        '''
        for task in self.tasks:
            if task.name == task_name:
                return task

        return None

    def get_first_task(self):
        '''
        Get the starting task
        :return: The starting task
        '''
        return self.tasks[0]

    def get_next_task(self, task=None):
        '''
        For a given task get the next task in the flow, there might be multiple next tasks if we are branching
        :param task: The task we want to get the next from
        :return: A list of tasks
        '''
        if not task:
            return self.get_first_task()

        else:
            return task.next

    # TODO: Need to add more logic here!
    def get_next_waiting(self, task=None):
        '''
        Get next task in WAITING state, in the case of WAITING-BRANCH the tasks could be in NULL state as we
        might not have selected a BRANCH
        :param task: If we want to start looking in a specific location in the flow
        :return: A list of tasks
        '''
        if not task:
            task = self.get_first_task()

        if task.state != "WAITING" or task.state != "NULL":
            return task
        else:
            self.get_next_waiting(task.next)

        return task

    def get_child_tasks(self, task):
        '''
        Get child tasks for a given task
        :param task: The task we want to get child tasks for
        :return: A list of tasks
        '''
        return task.childtasks

    def describe(self, task=None):
        '''
        Output a description of the flow
        :param task: Starting task, used when calling describe recursively
        :return: Nothing
        '''
        # Get the first task (the start task is always the first!)
        if not task:
            task = self.tasks[0]
            print("Flow: " + self.name)

        print(task.name + " : " + str(task.metadata))
        if task.state:
            print("- " + task.state)
        if task.dependson:
            print("- Depends on " + task.dependson.name)
        if task.parenttask:
            print("- Child task of " + task.parenttask.name)

        # If task has child tasks, let's get their info
        for child in task.childtasks:
            self.describe(child)

        # If task has next tasks, let's get their info
        for task in task.next:
            self.describe(task)


class Ticket:
    def __init__(self, flow_id=None, metadata={}, uuid=None, state=None, creation_time=None, tasks=None):
        self.uuid = uuid
        self.flow_id = flow_id
        self.metadata = metadata
        self.creation_time = creation_time
        self.state = state
        if not tasks:
            self.tasks = []
        else:
            self.tasks = tasks

    @classmethod
    def from_json(cls, json_data):
        '''
        Converts json representation of a ticket from Nasha to a Ticket object
        :param json_data: json containing ticket information
        :return: A Ticket object
        '''
        ticket = cls(flow_id=json_data['flow_id'], metadata=json_data['metadata'], uuid=json_data['id'],
                        creation_time=json_data['creation_time'], state=json_data['state'])

        ticket_tasks = json_data['ticket_tasks']
        for task in ticket_tasks:
            temp = Task(name=task['name'], uuid=task['task_id'], metadata=task['metadata'], state=task['state'])
            temp.ticket = ticket
            ticket.tasks.append(temp)

        return ticket

    def get_task_by_name(self, task_name):
        '''
        Gets a specific task from the ticket
        :return: A Task object if successful, None otherwise
        '''
        for task in self.tasks:
            if task.name == task_name:
                return task

        return None

    def get_task_by_id(self, task_id):
        '''
        Gets a specific task from the ticket
        :return: A Task object if successful, None otherwise
        '''
        for task in self.tasks:
            if task.uuid == task_id:
                return task

        return None


class Processor:
    def __init__(self, uuid=None, metadata={}):
        self.uuid = uuid
        self.metadata = metadata
        self.tasks = []

    @classmethod
    def from_json(cls, json_data):
        '''
        Converts json representation of a processor from Nasha to a Processor object
        :param json_data: json containing processor information
        :return: A Processor object
        '''
        processor = cls(uuid=json_data['id'], metadata=json_data['metadata'])

        tasks = json_data['task_ids']
        for task in tasks:
            temp = Task(uuid=task)
            processor.tasks.append(temp)

        return processor


class TicketJob:
    def __init__(self, flow_id=None, parent=None, config_data=None, job_data=None):
        self.flow_id = flow_id
        self.parent = parent
        self.config_data = config_data
        self.job_data = job_data

    @classmethod
    def from_json(cls, json_data):
        '''
        Converts json representation of a TicketJob from Nasha to a TicketJob object
        :param json_data: json containing ticketjob information
        :return: A TicketJob object
        '''
        taskjob = cls()
        if json_data['flow_id']:
            taskjob.flow_id = json_data['flow_id']
        if json_data['parent']:
            taskjob.parent = json_data['parent']
        if json_data['config_data']:
            taskjob.config_data = json_data['config_data']
        if json_data['job_data']:
            job_data = json_data['job_data']

        return taskjob


class TaskJob:
    def __init__(self, processor_id=None, ticket_id=None, task_id=None, state=None, ticket_data='{}', task_data='{}'):
        self.processor_id = processor_id
        self.ticket_id = ticket_id
        self.task_id = task_id
        self.state = state
        self.ticket_data = ticket_data
        self.task_data = task_data

    @classmethod
    def from_json(cls, json_data):
        '''
        Converts json representation of a TaskJob from Nasha to a TaskJob object
        :param json_data: json containing taskjob information
        :return: A TaskJob object
        '''
        taskjob = cls(ticket_id=json_data['ticket_id'], task_id=json_data['task_id'],
                      ticket_data=json_data['ticket_data'],task_data=json_data['task_data'])

        return taskjob

class Queue:
    def __init__(self, host=None, processor_id=None, queue_type='SQS'):
        self.host = host
        self.processor_id = processor_id
        self.connection = None
        self.channel = None
        self.queue_type = queue_type
        self.queue = None
        if not host and queue_type == 'RabbitMQ':
            raise Exception('Invalid RabbitMQ endpoint')
        if not processor_id:
            raise Exception('Processor uuid missing!')

    def connect(self):
        if self.queue_type == 'RabbitMQ':
            import pika
            self.connection = pika.BlockingConnection(pika.ConnectionParameters(host=self.host))
            print('Connected to ' + self.host)
            self.channel = self.connection.channel()
            self.channel.queue_declare(queue=self.processor_id)
            print('Listening on ' + self.processor_id)
        else:
            queue_name = self.processor_id + '.fifo'
            import boto3
            sqs = boto3.resource('sqs')
            print('Connecting to SQS:')
            self.queue = sqs.get_queue_by_name(QueueName=queue_name)
            print('Listening on '+queue_name)

    def listen(self, method):
        if not self.connection:
            self.connect()
        if self.queue_type == 'RabbitMQ':
            import pika
            self.channel.basic_consume(method,
                                  queue=self.processor_id,
                                  no_ack=True)

            print('I am a processor - '+self.processor_id)
            print('[*] Waiting for messages. To exit press CTRL+C')
            self.channel.start_consuming()
        else:
            import boto3
            while True:
                for message in self.queue.receive_messages():
                    message.delete()
                    method(None, None, None, message.body)
                    # message.body
                    # Let the queue know that the message is processed
