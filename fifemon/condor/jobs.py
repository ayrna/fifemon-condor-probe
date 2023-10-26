#!/usr/bin/python
import time
from collections import defaultdict, deque
import logging
from threading import Thread
import traceback
import re

import classad
import htcondor

logger = logging.getLogger(__name__)


def find_bin(value, bins):
    for b in bins:
        if value < b[0]:
            return b[1]
    return "longer"


def geteval(self, key, default=None):
    """
    get an attribute from the classad, returning default if not found.
    if the attribute is an expression, eval() it.
    """
    r = self.get(key,default)
    if isinstance(r, classad.ExprTree):
        return r.eval()
    return r

classad.ClassAd.geteval = geteval


def job_metrics(job_classad):
    """
    Returns a list of base metrics for the given job.
    """
    counters = []

    try:
        user_name = job_classad.get("Owner","unknown")
    except:
        user_name = "unknown"
    try:
        groups = re.findall(r'(?:group_)?(\w+)',job_classad.get("AccountingGroup","group_unknown"))
        exp_name = groups[0]
        subgroups = []
        if len(groups) > 1:
            # sometimes each user has an accounting group, we don't want those
            if groups[-1] == user_name:
                subgroups = groups[1:len(groups)-1]
            else:
                subgroups = groups[1:]
    except:
        exp_name = "unknown"
        subgroups = []

    if job_classad["JobUniverse"] == 7:
        counters = [".dag.totals"]
    elif job_classad["JobStatus"] == 1:
        counters = [".idle.totals"]
        if "DESIRED_usage_model" in job_classad:
            models = set(job_classad["DESIRED_usage_model"].split(","))
            if "DESIRED_Sites" in job_classad:
                sites = job_classad["DESIRED_Sites"].split(",")
                for s in sites:
                    counters.append(".idle.sites."+s)
                #if "Fermigrid" not in sites:
                #    models.discard("DEDICATED")
                #    models.discard("OPPORTUNISTIC")
            models_sorted = list(models)
            if len(models_sorted) == 0:
                models_sorted = ["impossible"]
            else:
                models_sorted.sort()
            counters.append(".idle.usage_models." + "_".join(models_sorted))
        else:
            counters.append(".idle.usage_models.unknown")
    elif job_classad["JobStatus"] == 2:
        counters = [".running.totals"]
        if "MATCH_GLIDEIN_Site" in job_classad:
            site = job_classad["MATCH_GLIDEIN_Site"]
            if site == "FNAL" and "MATCH_EXP_JOBGLIDEIN_ResourceName" in job_classad:
                site = job_classad["MATCH_EXP_JOBGLIDEIN_ResourceName"]
            counters.append(".running.sites." + site)
        else:
            counters.append(".running.sites.unknown")
    elif job_classad["JobStatus"] == 5:
        counters = [".held.totals"]
    else:
        counters = [".unknown.totals"]

    metrics = []
    for counter in counters:
        metrics.append("totals"+counter)
        metrics.append("experiments."+exp_name+".totals"+counter)
        metrics.append("experiments."+exp_name+".users."+user_name+counter)
        if len(subgroups) > 0:
            metrics.append("experiments."+exp_name+".subgroups."+".".join(subgroups)+'.'+counter)
        metrics.append("users."+user_name+counter)
    return metrics


def get_jobs(job_q, schedd_ad, constraint=True, attrs=['ClusterId','ProcId','JobStatus'], retry_delay=30, max_retries=4):
    retries=0
    while retries < max_retries:
        try:
            schedd = htcondor.Schedd(schedd_ad)
            results = schedd.query(constraint, attrs)
        except IOError:
            traceback.print_exc()
            logger.warning("Trouble communicating with schedd {0}, retrying in {1}s.".format(schedd_ad['Name'],retry_delay))
            retries += 1
            time.sleep(retry_delay)
            continue
        else:
            for r in results:
                job_q.append(r)
            return
    logger.error("Trouble communicating with schedd {0}, giving up.".format(schedd_ad['Name']))

def get_idle_jobs(job_q, schedd_ad, retry_delay=30, max_retries=4):
    get_jobs(job_q, schedd_ad, constraint='JobStatus==1', retry_delay=retry_delay, max_retries=max_retries,
            attrs=["ClusterId","ProcId","Owner",
                "AccountingGroup","JobStatus",
                "DESIRED_usage_model","DESIRED_Sites","JobUniverse",
                "QDate", "RequestMemory","RequestDisk","RequestCpus","Requestgpus"])

def get_running_jobs(job_q, schedd_ad, retry_delay=30, max_retries=4):
    get_jobs(job_q, schedd_ad, constraint='JobStatus==2', retry_delay=retry_delay, max_retries=max_retries,
            attrs=["ClusterId","ProcId","Owner",
                "MATCH_GLIDEIN_Site","MATCH_EXP_JOBGLIDEIN_ResourceName",
                "AccountingGroup","JobStatus",
                "JobUniverse","JobCurrentStartDate","RemoteUserCpu",
                "RequestMemory","ResidentSetSize",
                "RequestDisk","DiskUsage","RequestCpus",
                "AssignedGPus","GPUsProvisioned","Requestgpus"])

def get_held_jobs(job_q, schedd_ad, retry_delay=30, max_retries=4):
    get_jobs(job_q, schedd_ad, constraint='JobStatus==5', retry_delay=retry_delay, max_retries=max_retries,
            attrs=["ClusterId","ProcId","Owner",
                "AccountingGroup","JobStatus",
                "JobUniverse",
                "Requestgpus",
                "EnteredCurrentStatus"])


class Jobs(object):
    def __init__(self, interval, pool="localhost"):
        self.pool = pool
        self.interval = interval
        self.collector = htcondor.Collector(pool)
        self.bins=[(interval,  'recent'),
                   (3600,      'one_hour'),
                   (3600*4,    'four_hours'),
                   (3600*8,    'eight_hours'),
                   (3600*24,   'one_day'),
                   (3600*24*2, 'two_days'),
                   (3600*24*7, 'one_week')]


    def job_walltime(self, job_classad):
        now = int(time.time())
        start = job_classad.get("JobCurrentStartDate",now)
        return (now-start)*job_classad.geteval("RequestCpus",1)

    def job_cputime(self, job_classad):
        return job_classad.get("RemoteUserCpu",0)

    def job_bin(self, job_classad):
        bin = None
        if job_classad["JobStatus"] == 1:
            if "QDate" in job_classad:
                qage = int(time.time())-job_classad["QDate"]
                bin = ".count_"+find_bin(qage, self.bins)
            else:
                bin = ".count_unknown"
        elif job_classad["JobStatus"] == 2:
            walltime = self.job_walltime(job_classad)
            if walltime > 0:
                bin = ".count_"+find_bin(walltime, self.bins)
            else:
                bin = ".count_unknown"
        elif job_classad["JobStatus"] == 5:
            if "EnteredCurrentStatus" in job_classad:
                holdage = int(time.time())-job_classad["EnteredCurrentStatus"]
                bin = ".count_holdage_"+find_bin(holdage, self.bins)
            else:
                bin = ".count_holdage_unknown"
        return bin


    def get_job_count(self, retry_delay=30, max_retries=4, schedd_constraint=True):
        if callable(schedd_constraint):
            schedd_constraint=schedd_constraint(self.collector)
        try:
            ads = self.collector.query(htcondor.AdTypes.Schedd,schedd_constraint)
        except:
            logger.error("Trouble getting pool {0} schedds.".format(self.pool))
            return None

        job_q = deque()
        ## spawn off workers to query each schedd and put metrics for each job in a queue
        for a in ads:
            get_idle_jobs(job_q,a,retry_delay,max_retries)
            get_running_jobs(job_q,a,retry_delay,max_retries)
            get_held_jobs(job_q,a,retry_delay,max_retries)

        logger.info("Processing jobs")
        counts = defaultdict(int)
        for r in job_q:
            for m in job_metrics(r):
                counts[m+".count"] += 1

                bin = self.job_bin(r)
                if bin is not None:
                    counts[m+bin] += 1

                walltime = self.job_walltime(r)
                cputime = self.job_cputime(r)
                if walltime > 0 and cputime > 0:
                    counts[m+".walltime"] += walltime
                    counts[m+".cputime"] += cputime
                    counts[m+".efficiency"] = max(min(counts[m+".cputime"]/counts[m+".walltime"]*100,100),0)
                    counts[m+".wastetime"] = counts[m+".walltime"]-counts[m+".cputime"]
                    if counts[m+".count"] > 0:
                        counts[m+".wastetime_avg"] = counts[m+".wastetime"]/counts[m+".count"]

                ## one standard slot == 1 cpu and 2000 MB of memory (undefined amount of disk)
                ## one standar gpu slot == 1 gpu (undefined the rest)
                std_slots = 1
                std_slots_gpu = 0
                if "RequestCpus" in r:
                    cpus = r.eval("RequestCpus")
                    try:
                        counts[m+".cpu_request"] += cpus
                        std_slots = max(std_slots,cpus)
                    except:
                        pass
                if "Requestgpus" in r:
                    std_slots_gpu = 1
                    gpus = r.eval("Requestgpus")
                    try:
                        counts[m+".gpu_request"] += gpus
                        std_slots_gpu = max(std_slots_gpu,gpus)
                    except:
                        pass
                if "RequestMemory" in r:
                    mem = r.eval("RequestMemory")
                    try:
                        counts[m+".memory_request_b"] += mem*1024.0*1024.0
                        std_slots = max(std_slots,mem/2000.0)
                    except:
                        pass
                if "RequestDisk" in r:
                    try:
                        counts[m+".disk_request_b"] += r.eval("RequestDisk")*1024
                    except:
                        pass
                counts[m+".std_slots"] += std_slots
                counts[m+".std_slots_gpu"] += std_slots_gpu

                if r["JobStatus"] == 2:
                    if "GPUsUsage" in r:
                        counts[m+".gpus_usage"] += r.eval("GPUsUsage")
                    if "GPUsProvisioned" in r:
                        counts[m+".gpus_provisioned"] += r.eval("GPUsProvisioned")
                    if "ResidentSetSize" in r:
                        counts[m+".memory_usage_b"] += r.eval("ResidentSetSize")*1024
                    if "DiskUsage" in r:
                        counts[m+".disk_usage_b"] += r.eval("DiskUsage")*1024

        return counts
