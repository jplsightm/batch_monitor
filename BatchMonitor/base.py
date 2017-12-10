import boto3
from datetime import datetime

class Base(object):
    """
    This will serve as the base of batch monitoring for AWS. This class will provide the ability to connect and define
    some basic ability to monitor state
    """
    def __init__(self, _id, _key, _region='ap-southeast-1'):
        self.batch_client = boto3.client(service_name='batch', region_name=_region,
                                         aws_access_key_id=_id, aws_secret_access_key=_key)
        self.valid_states = ['SUBMITTED', 'PENDING', 'RUNNABLE', 'STARTING', 'RUNNING', 'SUCCEEDED', 'FAILED']

    def state_count(self, queue, state, get_ids=False):
        """
        Get a count of how many job jobs are in a specific state

        :param queue:
        :param state:
        :param get_ids:
        :return:
        """

        _time = datetime.now()

        if state not in self.valid_states:
            raise ValueError('state is not valid. Refer to Amazon docs for valid state: '
                             'http://docs.aws.amazon.com/batch/latest/userguide/job_states.html')

        next_token = True
        token_value = None
        job_ids = {}

        while next_token:

            # query the queue
            if token_value is None:
                rs = self.batch_client.list_jobs(jobQueue=queue, jobStatus=state)
            elif token_value:
                # print token_fails
                rs = self.batch_client.list_jobs(jobQueue=queue, jobStatus=state, nextToken=token_value)
            else:
                rs = {}

            if token_value is None or token_value:
                jobs = {job['jobId'] : job['jobName'] for job in rs['jobSummaryList']}
                job_ids.update(jobs)

            if 'nextToken' in rs:
                token_value = rs['nextToken']
            else:
                token_value = False

            if token_value is False:
                next_token = False

        if not get_ids:
            _ids = None
        else:
            _ids = job_ids

        return {'_logged_time': _time, '_operation': 'state_count', 'state': state, 'count': len(job_ids),
                'job_ids': _ids}

    def failed_jobs_by_name(self, queue, external_succeeded=[], external_failed=[]):
        """
        This will identify jobs by name that have failed. This compares a list of failed jobs to jobs that have
        succeeded. This also allows for external lists (an iterable) that can be combined with job information from aws.
        The rational for this is because AWS Batch will remove historical information after 24 hours, in many cases this
        is okay. When it is not it can be nasty surprise when you forget to backup information and you have a
        metephorical gun to your head

        :param queue:
        :param external_succeeded
        :param external_failed
        :return:
        """

        failed_jobs = self.state_count(queue, 'FAILED', True)['job_ids']
        successful_jobs = self.state_count(queue, 'SUCCEEDED', True)['job_ids']

        succeeded = successful_jobs.values() + external_succeeded
        failed = failed_jobs.values() + external_failed

        unique_failed_job_names = [name for name in failed if name not in succeeded]

        return list(set(unique_failed_job_names))

    def status_summary(self, queue):
        """

        :param queue:
        :return:
        """

        times = []
        count_by_status = {}

        for status in self.valid_states:
            _state = self.state_count(queue, status)
            count_by_status[status] = _state['count']
            times.append(_state['_logged_time'])

        return {'time_range': (min(times), max(times)), 'states': count_by_status}
