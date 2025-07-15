#cd C:\Users\samut\OneDrive\Documents\GitHub\developersInactivityAnalysisCOPY\Extractors
#python CommitExtractor.py

### IMPORT EXCEPTION MODULES
from requests.exceptions import Timeout
from github import GithubException, UnknownObjectException, IncompletableObject

### IMPORT SYSTEM MODULES
from github import Github
import os, logging, pandas, csv, tempfile, shutil, functools
from datetime import datetime, timezone
from tqdm import tqdm, TqdmWarning, tqdm
from pathlib import Path
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
import signal, threading


### IMPORT CUSTOM MODULES
import sys
sys.path.append('../')
import Settings as cfg
import Utilities as util
from pathlib import Path
import subprocess, tempfile, shutil, logging
from truckfactor.compute import main as compute_tf
import portalocker              # pip install portalocker
import warnings
warnings.filterwarnings("ignore")
from git import Repo, exc as git_exc



### DEFINE CONSTANTS
COMPLETE = "COMPLETE"
STOP_EVENT = threading.Event()

def _signal_handler(sig, frame):
    print("\n⏹️  Stop requested – finishing current API call …")
    STOP_EVENT.set()              # tell workers to exit
signal.signal(signal.SIGINT, _signal_handler)   # Ctrl-C

def getExtractionStatus(folder, statusFile):
    status = "NOT-STARTED"
    if(statusFile in os.listdir(folder)):
        with open(os.path.join(folder, statusFile)) as f:
            content = f.readline().strip()
        status, _ = content.split(';')
    return status



def runALLExtractionRoutine(organizationFolder, organization, project, extraction_type= True ):
    
    workingFolder = (os.path.join(organizationFolder, project))
    os.makedirs(workingFolder, exist_ok=True)

    if getExtractionStatus(workingFolder, "_extractionStatus.tmp") == COMPLETE:
        print(f"Extraction completed for {project}")
        return
    
    g0       = Github(util.getSpisificToken(0))
    repo     = g0.get_repo(f"{organization}/{project}")
    project_start_dt = repo.created_at
    collection_day   = datetime.strptime(cfg.data_collection_date, "%Y-%m-%d")



    #one of the main Repo and one for the side repos.
    #we only collect commtis from non main repo
    if extraction_type:
        work_orders = [
            {"kind": "Issue", "token_idx": 0},
            {"kind": "PR"   , "token_idx": 1},
            {"kind": "Commit", "token_idx": 2},
            {"kind": "Commit", "token_idx": 3}
        ]


    else:
        work_orders = [
            {"kind": "Commit", "token_idx": 0},
            {"kind": "Commit", "token_idx": 1},
            {"kind": "Commit", "token_idx": 2},
            {"kind": "Commit", "token_idx": 3},
        ]
        

    total_commits = repo.get_commits(
            since=project_start_dt, until=collection_day).totalCount
    
    total_prs     = repo.get_pulls(
            state="all").totalCount              # GitHub API ignores date filters
        
    query = """
    query($owner: String!, $name: String!) {
    repository(owner: $owner, name: $name) {
        issues(states:[OPEN, CLOSED])   { totalCount }
    }
    }
    """
    vars = {"owner": repo.owner.login, "name": repo.name}
    data = repo.requester.graphql_query(query=query, variables=vars)[1]
    num_items: int = data["data"]["repository"]["issues"]["totalCount"]


    pb_commit = tqdm(total=total_commits, desc="Commits", position=0, leave=True)
    pb_pr     = tqdm(total=total_prs,     desc="PRs    ", position=1, leave=True)
    pb_issue  = tqdm(total=num_items,     desc="Issues ", position=2, leave=True)


    bars = {"Commit": pb_commit, "Issue": pb_issue,  "PR": pb_pr}


    with ThreadPoolExecutor(max_workers=4) as pool:
        try:
            future_map = {
                pool.submit(extraction_worker, order, organizationFolder, organization, project,
                            project_start_dt, collection_day, bars[order["kind"]]
                            ): order for order in work_orders}

            for fut in as_completed(future_map):
                fut.result()      # propagate real errors
        except KeyboardInterrupt:
            # we only land here if SIGINT hit the *main* thread
            STOP_EVENT.set()
        finally:
            pool.shutdown(wait=False, cancel_futures=True)
    
    print("Running the Extraction. \n Connection Done. \n Logging in {}".format(logfile))

    return


def extraction_worker(order, org_folder, org, project,
                      project_start_dt, collection_day, pbar):

    token = util.getSpisificToken(order["token_idx"])
    g = Github(token)
    g.per_page = cfg.items_per_page
    repo_name = f"{org}/{project}"
    working   = os.path.join(org_folder, project)

    if order["kind"] == "Issue":
        return updateIssueListFile(
            g, token, repo_name, project_start_dt, collection_day,
            working, pbar                    
        )

    if order["kind"] == "PR":
        return updatePRListFile(
            g, token, repo_name, project_start_dt, collection_day,
            working, pbar
        )

    # Commit
    return updateCommitListFile(
        g, token, repo_name, project_start_dt, collection_day,
        working, pbar
    )

    
    

def updateCommitListFile(g, token, repoName, start_date, end_date, workingFolder, pbar, position = 0):
    
    commits_csv        = cfg.commit_list_file_name
    next_page           = cfg.next_page_commits
    last_page_save      = cfg.last_page_commits
    excl_tmp           = "_excludedNoneTypeCommits.tmp"
    status_tmp         = "_extractionStatusCommits.tmp"

    commit_cols = ["repo", "created_at", "author_id","author_name","author_email",  "committer_id",
                   "sha", "filename_list",
                   "fileschanged_count", "additions_sum", "deletions_sum"]


    status = getExtractionStatus(workingFolder, status_tmp)
    if status == COMPLETE:
        return g, token

    os.makedirs(workingFolder, exist_ok=True)
    with open(Path(workingFolder, status_tmp), "w") as fh:
        fh.write(f"INCOMPLETE;{datetime.today():%Y-%m-%d %H:%M:%S}")

    commits_df      = (pandas.read_csv(Path(workingFolder, commits_csv),
                                   sep=cfg.CSV_separator)
                       if Path(workingFolder, commits_csv).exists()
                       else pandas.DataFrame(columns=commit_cols))
    
    excluded        = (pandas.read_csv(Path(workingFolder, excl_tmp),
                                   sep=cfg.CSV_separator)
                       if Path(workingFolder, excl_tmp).exists()
                       else pandas.DataFrame(columns=["sha"]))
    def mark_excluded(sha: str):
        """Add sha to excluded df + set (idempotent)."""
        if sha and sha not in processed_excluded:
            util.add(excluded, [sha])
            processed_excluded.add(sha)

    processed_commits   = set(commits_df.sha)
    processed_excluded  = set(excluded.sha)

    g, token, search_limit ,   reset_time = util.getSameToken(g, token, position)
    repo     = g.get_repo(repoName)
    commits_pl = repo.get_commits(since=start_date, until=end_date)

    last_page   = int(commits_pl.totalCount / cfg.items_per_page)
    last_page   = max(last_page, 0)
    start_page  = (util.getLastPageRead(Path(workingFolder, last_page_save))
                   if Path(workingFolder, last_page_save).exists()
                   else 0)
    total_pages = last_page - start_page + 1
    current_page = start_page

    # ── main extraction loop with retry wrapper ─────────────────────────
    exception_thrown = True
    count = 0
    commit_count = 0

    while exception_thrown:
        exception_thrown = False

        try:
            #while current page is not larger than the last page
            while current_page <= last_page:

                current_page = updatePageFile(workingFolder, next_page)
                # progress bar / rate-limit housekeeping
                
                page_commits = commits_pl.get_page(current_page)   # <-- new var

                g, token, search_limit, reset_time = util.getSameToken(g, token, position)
                
                add_sum = del_sum = 0

                # ── iterate PRs on this page ────────
                for commit in page_commits:
                    if STOP_EVENT.is_set():
                        break
                    pbar.update(1)
                    commit_count += 1
                    sha = commit.sha
                    if sha is None:
                        tqdm.write(f"Commit has no sha or name, skipping.")
                        continue
                    if sha in processed_commits:
                        tqdm.write(f"Commit {sha} already processed, skipping.") 
                        continue
                    if sha in processed_excluded:
                        tqdm.write(f"Commit {sha} already excluded, skipping.")
                        continue

                    # handle ghost / deleted users
                    author_id    = (commit.author.login
                                    if commit.author else "NA")
                    author_name = (commit.author.name
                                    if commit.author else "NA")
                    committer_id = (commit.committer.login
                                    if commit.committer else "NA")
                    author_email = commit.commit.author.email


                    if author_email is None and author_name is None and author_id is None:
                        util.add(excluded, [sha])
                        processed_excluded.add(sha)
                        tqdm.write(f"Commit {sha} has no author, skipping.")
                        continue

                    g, token, search_limit ,   reset_time = util.getSameToken(g, token, position)

                    commit_created_at = commit.commit.author.date
                    
                    files             = commit.files
                    filenames         = [f.filename for f in files]
                    fchg_cnt          = len(filenames)
                    adds              = sum(f.additions for f in files)
                    dels              = sum(f.deletions for f in files)

                    add_sum += adds
                    del_sum += dels

                    new_commit = [repo, commit_created_at, author_id, author_name, author_email,
                         committer_id, sha, "|".join(filenames), fchg_cnt, adds, dels]
                    util.add(
                        commits_df, new_commit
                    )

                    #pbar.set_postfix({
                    #    "commit_count": commit_count,
                    #    "page": f"{current_page}/{last_page}",
                    #    "search_limit": search_limit,
                    #    "user": author_id,
                    #    "reset_time": reset_time.strftime("%H:%M:%S"),
                    #    "repo": repoName.split("/")[-1]}, refresh=True)
                    
                    processed_commits.add(sha)

                _flush_data_to_csv(commits_df = commits_df, prs_df = None, prs_comments_df = None, excluded = excluded, workingFolder = workingFolder, page = current_page, save_last_page = last_page_save, save_current_page = next_page, is_page_finished = False)
                commits_df.drop(commits_df.index, inplace=True)
                time.sleep(0.1)  # avoid hitting the API too hard
        except KeyboardInterrupt:
            # ── 1.  user pressed Ctrl-C  ────────────────────────────────
            logging.warning("Extraction interrupted by user – saving progress and exiting.")
            _flush_data_to_csv(commits_df = commits_df, prs_df = None, prs_comments_df = None, excluded = excluded, workingFolder = workingFolder, page = current_page, save_last_page = last_page_save, save_current_page = next_page, is_page_finished = False)
            commits_df.drop(commits_df.index, inplace=True)
            raise  

        except (UnknownObjectException, GithubException, AttributeError) as e:
            count += 1
            logging.warning(f"Extraction interrupted: {e}")

            # ── 2.  rate-limit or abuse-limit  ──────────────────────────
            if isinstance(e, GithubException) and e.status in (403, 429):
                delay = 300
                logging.warning(f"Rate-limited – sleeping {delay}s")
                time.sleep(delay)
                continue

            # ── 3.  forbidden but not back-off related  ─────────────────
            if isinstance(e, GithubException) and e.status == 403:
                mark_excluded(sha)

            # ── 4.  write whatever we have so far  ──────────────────────
            _flush_data_to_csv(commits_df = commits_df, prs_df = None, prs_comments_df = None, excluded = excluded, workingFolder = workingFolder, page = current_page, save_last_page = last_page_save, save_current_page = next_page, is_page_finished = False)
            exception_thrown = True

        except Exception as e:
            logging.warning(f"Unhandled exception: {e}")
            _flush_data_to_csv(commits_df = commits_df, prs_df = None, prs_comments_df = None, excluded = excluded, workingFolder = workingFolder, page = current_page, save_last_page = last_page_save, save_current_page = next_page, is_page_finished = False)
            commits_df.drop(commits_df.index, inplace=True)
            raise
    pbar.close()

    # ── final flush / bookkeeping --------------------------------------
    _flush_data_to_csv(commits_df = commits_df, prs_df = None, prs_comments_df = None, excluded = excluded, workingFolder = workingFolder, page = current_page, save_last_page = last_page_save, save_current_page = next_page, is_page_finished = False)
    commits_df.drop(commits_df.index, inplace=True)

    if len(excluded):
        excluded.to_csv(Path(workingFolder, excl_tmp),
                        sep=cfg.CSV_separator, index=False,
                        lineterminator="\n")

    with open(Path(workingFolder, status_tmp), "w") as fh:
        fh.write(f"COMPLETE;{cfg.data_collection_date}")

    logging.info("Commit + PR extraction COMPLETE for %s", repoName)
    return g, token

def updatePRListFile(g, token, repoName, start_date, end_date, workingFolder, pbar, position = 1):
    prs_csv            = cfg.PR_list_file_name
    prs_comments_csv   = cfg.prs_comments_csv
    next_page           = cfg.next_page
    last_page_save          = cfg.last_page
    excl_tmp           = "_excludedNoneType.tmp"
    status_tmp         = "_extractionStatus.tmp"

    pr_cols     = ["repo", "created_at", "created_by", "PR_id",
                   "state", "merged", "closed_at", "merged_at"]
                
    prcom_cols  = ["repo", "created_at", "created_by", "PR_id",
                   "comment_id", "event"]

    # ── early-exit if already complete ──────────────────────────────────
    status = getExtractionStatus(workingFolder, status_tmp)
    if status == COMPLETE:
        return g, token

    os.makedirs(workingFolder, exist_ok=True)
    with open(Path(workingFolder, status_tmp), "w") as fh:
        fh.write(f"INCOMPLETE;{datetime.today():%Y-%m-%d %H:%M:%S}")

    # ── load existing CSVs (or empty frames) ────────────────────────────
    
    prs_df          = (pandas.read_csv(Path(workingFolder, prs_csv),
                                   sep=cfg.CSV_separator)
                       if Path(workingFolder, prs_csv).exists()
                       else pandas.DataFrame(columns=pr_cols))

    prs_comments_df = (pandas.read_csv(Path(workingFolder, prs_comments_csv),
                                   sep=cfg.CSV_separator)
                       if Path(workingFolder, prs_comments_csv).exists()
                       else pandas.DataFrame(columns=prcom_cols))

    excluded        = (pandas.read_csv(Path(workingFolder, excl_tmp),
                                   sep=cfg.CSV_separator)
                       if Path(workingFolder, excl_tmp).exists()
                       else pandas.DataFrame(columns=["sha"]))
    def mark_excluded(sha: str):
        """Add sha to excluded df + set (idempotent)."""
        if sha and sha not in processed_excluded:
            util.add(excluded, [sha])
            processed_excluded.add(sha)
    # ── reusable sets for “skip-if-seen” ────────────────────────────────
    processed_comments  = set(prs_comments_df.comment_id)
    processed_excluded  = set(excluded.sha)

    # ── GitHub paging setup ─────────────────────────────────────────────
    g, token, search_limit ,   reset_time = util.getSameToken(g, token, position)
    repo     = g.get_repo(repoName)
    pulls = repo.get_pulls(state="all", sort="created", direction="desc")
    

    last_page   = int(pulls.totalCount / cfg.items_per_page)
    last_page   = max(last_page, 0)
    start_page  = (util.getLastPageRead(Path(workingFolder, cfg.last_page))
                   if Path(workingFolder, cfg.last_page).exists()
                   else 0)
    current_page = start_page

    # ── main extraction loop with retry wrapper ─────────────────────────
    page = start_page  # make sure it's defined for finally/_flush
    exception_thrown = True
    count = 0
    pr_count =0
    commit_count = 0
    comments_count = 0

    def handle_github_backoff(exc, attempt=0, base_delay=60, cap=900):
            """
            Decide how long to sleep after a GithubException.
            Returns the delay in seconds (may be 0).
            """
            hdrs = getattr(exc, "headers", {}) or {}

            # 1. Retry-After wins
            if "Retry-After" in hdrs:
                return int(hdrs["Retry-After"])

            # 2. Primary rate-limit exhausted
            if hdrs.get("X-RateLimit-Remaining") == "0":
                reset = int(hdrs.get("X-RateLimit-Reset", 0))
                return max(reset - int(time.time()), 1)

            # 3. Secondary limit: exponential back-off
            return min(base_delay * (2 ** attempt), cap)
    
    while exception_thrown:
        exception_thrown = False
        try:
            #while current page is not larger than the last page
            while current_page <= last_page and not STOP_EVENT.is_set():

                current_page = updatePageFile(workingFolder, next_page)
                # progress bar / rate-limit housekeeping
                g, token, search_limit, reset_time = util.getSameToken(g, token, position)

                pulls_page = pulls.get_page(current_page)

                # ── iterate PRs on this page ───────────────────────────
                
                for pr in pulls_page:
                    
                    pbar.update(1)
                    g, token, search_limit ,   reset_time = util.getSameToken(g, token, position)
                    pr_count += 1
                    
                    pr_id      = pr.number
                    repo_name  = repo.full_name

                    # PR meta row -------------------------------------------------
                    util.add(
                        prs_df,
                        [repo_name, pr.created_at, pr.user.login
                         if pr.user else None,
                         pr_id, pr.state, bool(pr.merged),
                         pr.closed_at, pr.merged_at]
                    )
                
                    # PR comments -------------------------------------------------
                    for cmt in pr.get_comments():
                        if cmt.id in processed_comments:
                            continue
                        comments_count += 1
                        util.add(
                            prs_comments_df,
                            [repo_name, cmt.created_at,
                             cmt.user.login if cmt.user else None,
                             pr_id, cmt.id, "comment"]
                        )
                        processed_comments.add(cmt.id)

                    # PR reviews --------------------------------------------------
                    for rvw in pr.get_reviews():
                        if rvw.id in processed_comments:
                            continue
                        comments_count += 1
                        util.add(
                            prs_comments_df,
                            [repo_name, rvw.submitted_at,
                             rvw.user.login if rvw.user else None,
                             pr_id, rvw.id, "review"]
                        )
                        processed_comments.add(rvw.id)
                    if STOP_EVENT.is_set():
                        break 
                _flush_data_to_csv( commits_df= None, prs_df = prs_df, prs_comments_df = prs_comments_df,
                            excluded= excluded, workingFolder= workingFolder,  save_last_page = last_page_save , save_current_page = next_page , page= current_page, is_page_finished = True)
                prs_df.drop(prs_df.index, inplace=True)
                prs_comments_df.drop(prs_comments_df.index, inplace=True)
                time.sleep(0.1)  # avoid hitting the API too hard
        except KeyboardInterrupt:
            # ── 1.  user pressed Ctrl-C  ────────────────────────────────
            logging.warning("Extraction interrupted by user – saving progress and exiting.")
            _flush_data_to_csv( commits_df= None, prs_df = prs_df, prs_comments_df = prs_comments_df,
                            excluded= excluded, workingFolder= workingFolder,  save_last_page = last_page_save , save_current_page = next_page , page= current_page, is_page_finished = False)
            prs_df.drop(prs_df.index, inplace=True)
            prs_comments_df.drop(prs_comments_df.index, inplace=True)
            raise  

        except (UnknownObjectException, GithubException, AttributeError) as e:
            count += 1
            logging.warning(f"Extraction interrupted: {e}")

            # ── 2.  rate-limit or abuse-limit  ──────────────────────────
            if isinstance(e, GithubException) and e.status in (403, 429):
                delay = handle_github_backoff(e, attempt=count)
                logging.warning(f"Rate-limited – sleeping {delay}s")
                time.sleep(delay)
                continue

            # ── 3.  forbidden but not back-off related  ─────────────────
            if isinstance(e, GithubException) and e.status == 403:
                mark_excluded(pr_id)

            # ── 4.  write whatever we have so far  ──────────────────────
            _flush_data_to_csv( commits_df= None, prs_df = prs_df, prs_comments_df = prs_comments_df,
                            excluded= excluded, workingFolder= workingFolder,  save_last_page = last_page_save , save_current_page = next_page , page= current_page, is_page_finished = False)
            prs_df.drop(prs_df.index, inplace=True)
            prs_comments_df.drop(prs_comments_df.index, inplace=True)
            exception_thrown = True

        except Exception as e:
            logging.warning(f"Unhandled exception: {e}")
            _flush_data_to_csv( commits_df= None, prs_df = prs_df, prs_comments_df = prs_comments_df,
                            excluded= excluded, workingFolder= workingFolder,  save_last_page = last_page_save , save_current_page = next_page , page= current_page, is_page_finished = False)            
            prs_df.drop(prs_df.index, inplace=True)
            prs_comments_df.drop(prs_comments_df.index, inplace=True)

            raise
        
    
    pbar.close()

    # ── final flush / bookkeeping --------------------------------------
    _flush_data_to_csv( commits_df= None, prs_df = prs_df, prs_comments_df = prs_comments_df,
                            excluded= excluded, workingFolder= workingFolder,  save_last_page = last_page_save , save_current_page = next_page , page= current_page, is_page_finished = False)
    prs_df.drop(prs_df.index, inplace=True)
    if len(excluded):
        excluded.to_csv(Path(workingFolder, excl_tmp),
                        sep=cfg.CSV_separator, index=False,
                        lineterminator="\n")

    with open(Path(workingFolder, status_tmp), "w") as fh:
        fh.write(f"COMPLETE;{cfg.data_collection_date}")

    logging.info("Commit + PR extraction COMPLETE for %s", repoName)
    return g, token

def updateIssueListFile(
        g, token,
        repoName, start_date, end_date,
        workingFolder,
        pbar,
        position = 2):
    """Writes the list of the Issues for the given repository"""
    last_issue = 0  

    next_page           = cfg.next_page_issues
    last_page_save          = cfg.last_page_issues
    excl_tmp = "_excludedNoneType_Issues.tmp"
    status_tmp = "_extractionStatus_Issues.tmp"

    #commit file names
    issues_csv = cfg.issue_list_file_name
    events_csv = cfg.issue_events_list_file_name
    issues_comments_csv = cfg.issue_comments_list_file_name
    timeline_csv = cfg.issue_timeline_file_name

    save_tmp    = "_saveFile_Issues.tmp"
    excl_tmp    = "_excludedNoneType_Issues.tmp"
    status_tmp  = "_extractionStatus_Issues.tmp"

    issues_cols = [          "repo", "created_at", "created_by", "issue_id", "title", "labels", "state", "body", "assignees", "milestone" ]
    issue_events_cols = [    "repo", "created_at", "created_by", "issue_id", "event_id", "event" ]
    issues_comments_cols = [ "repo", "created_at", "created_by", "issue_id", "comment_id", "body" ]
    timeline_cols = [        "repo", "created_at", "created_by", "issue_id", "event_id", "event" ] 
    

    status = getExtractionStatus(workingFolder, status_tmp)
    if status == COMPLETE:                     # already done
        return g, token

    os.makedirs(workingFolder, exist_ok=True)
    with open(os.path.join(workingFolder, status_tmp), "w") as fh:
        fh.write(f"INCOMPLETE;{datetime.today():%Y-%m-%d %H:%M:%S}")
    save_path = os.path.join(workingFolder, "_saveFile_Issues.tmp")
    if os.path.exists(save_path):
        last_issue = int(open(save_path).readline().split(';')[0])


    excluded = pandas.read_csv(os.path.join(workingFolder, excl_tmp), sep=cfg.CSV_separator) if excl_tmp in os.listdir(workingFolder) else pandas.DataFrame(columns=["issue_id"])

    issues_df = pandas.read_csv(os.path.join(workingFolder, issues_csv), sep=cfg.CSV_separator) if os.path.exists(os.path.join(workingFolder, issues_csv)) else pandas.DataFrame(columns=issues_cols)
    events_df = pandas.read_csv(os.path.join(workingFolder, events_csv), sep=cfg.CSV_separator) if os.path.exists(os.path.join(workingFolder, events_csv)) else pandas.DataFrame(columns=issue_events_cols)
    comments_df = pandas.read_csv(os.path.join(workingFolder, issues_comments_csv), sep=cfg.CSV_separator) if os.path.exists(os.path.join(workingFolder, issues_comments_csv)) else pandas.DataFrame(columns=issues_comments_cols)
    timeline_df = pandas.read_csv(os.path.join(workingFolder, timeline_csv), sep=cfg.CSV_separator) if os.path.exists(os.path.join(workingFolder, timeline_csv)) else pandas.DataFrame(columns=timeline_cols)


    # commits total-count (for page math)
    processed_issues = set(issues_df.issue_id)
    processed_events = set(events_df.event_id)
    processed_comments = set(comments_df.comment_id)
    processed_timeline = set(timeline_df.event_id)
    
    _flush_issues_data_to_csv(
        issues_df, events_df, comments_df, timeline_df, excluded,
        workingFolder, save_tmp, last_issue=0,
        page=0,  # replace with real page
        save_last_page=cfg.last_page_issues,
        save_current_page=cfg.next_page_issues,
        is_page_finished=False
    )

    g, token, search_limit, reset_time = util.getSameToken(g, token, position)
    repo     = g.get_repo(repoName)
    issues_iter = repo.get_issues(state="all", sort="created", direction="dec")
    
    query = """
    query($owner: String!, $name: String!) {
    repository(owner: $owner, name: $name) {
        issues(states:[OPEN, CLOSED])   { totalCount }
    }
    }
    """
    vars = {"owner": repo.owner.login, "name": repo.name}
    data = repo.requester.graphql_query(query=query, variables=vars)[1]
    num_items: int = data["data"]["repository"]["issues"]["totalCount"]
        
    last_page   = int(num_items / cfg.items_per_page)
    last_page   = max(last_page, 0)
    start_page  = (util.getLastPageRead(Path(workingFolder, cfg.last_page))
                   if Path(workingFolder, cfg.last_page).exists()
                   else 0)
    current_page = start_page



    exception_thrown = True

    while exception_thrown:
        exception_thrown = False
        try:
            
            while current_page <= last_page and not STOP_EVENT.is_set():

                current_page = updatePageFile(workingFolder, next_page)


                issue_page = issues_iter.get_page(current_page)
                
                issues_df = pandas.read_csv(os.path.join(workingFolder, issues_csv), sep=cfg.CSV_separator) if issues_csv in os.listdir(workingFolder) else pandas.DataFrame(columns=issues_cols)
                events_df = pandas.read_csv(os.path.join(workingFolder, events_csv), sep=cfg.CSV_separator) if events_csv in os.listdir(workingFolder) else pandas.DataFrame(columns=issue_events_cols)
                comments_df = pandas.read_csv(os.path.join(workingFolder, issues_comments_csv), sep=cfg.CSV_separator) if issues_comments_csv in os.listdir(workingFolder) else pandas.DataFrame(columns=issues_comments_cols)
                timeline_df = pandas.read_csv(os.path.join(workingFolder, timeline_csv), sep=cfg.CSV_separator) if timeline_csv in os.listdir(workingFolder) else pandas.DataFrame(columns=timeline_cols)


                for issue in issue_page:
                    pbar.update(1)
                    g, token, search_limit ,  reset_time = util.getSameToken(g, token, position)
                    if STOP_EVENT.is_set():
                        break
                    if issue.user is None:   # ghost commit
                        util.add(excluded, [issue.id])
                        excluded.add(issue.id)
                        continue
                    
                    if issue.id < last_issue:
                        continue
                    if issue.id in processed_issues:
                        continue
                    if issue.id in excluded.issue_id.values:
                        continue
                    
                    processed_issues.add(issue.id)
                    issues_df.loc[len(issues_df)] = {
                        "repo": repoName,
                        "created_at": issue.created_at,
                        "created_by": issue.user.login,
                        "issue_id": issue.id,
                        "title": issue.title,
                        "labels": ",".join([label.name for label in issue.labels]),
                        "state": issue.state,
                        "body": issue.body or "",
                        "assignees": ",".join([assignee.login for assignee in issue.assignees]),
                        "milestone": issue.milestone.title if issue.milestone else "",
                    }

                    for comment in issue.get_comments():
                        if comment.id in processed_comments:
                            continue
                        processed_comments.add(comment.id)
                        comments_df.loc[len(comments_df)] = {
                            "repo": repoName,
                            "created_at": comment.created_at,
                            "created_by": comment.user.login if comment.user else "",
                            "issue_id": issue.id,
                            "comment_id": comment.id,
                            "body": comment.body or ""
                        }
                    for event in issue.get_events():
                        if event.id in processed_events:
                            continue
                        processed_events.add(event.id)
                        events_df.loc[len(events_df)] = {
                            "repo": repoName,
                            "created_at": event.created_at,
                            "created_by": event.actor.login if event.actor else "",
                            "issue_id": issue.id,
                            "event_id": event.id,
                            "event": event.event
                        }
                    for interaction in issue.get_timeline():
                        if interaction.id in processed_timeline:
                            continue
                        processed_timeline.add(interaction.id)

                        timeline_df.loc[len(timeline_df)] = {
                            "repo": repoName,
                            "created_at": interaction.created_at,
                            "created_by": interaction.actor.login if interaction.actor else "",
                            "issue_id": issue.id,
                            "event_id": interaction.id,
                            "event": interaction.event
                        }

                    last_issue = issue.id
                    excluded.loc[len(excluded)] = {
                        "issue_id": issue.id
                    }
                


            # ----- exception handlers unchanged ------------------------
            _flush_issues_data_to_csv(issues_df, events_df, comments_df, timeline_df, excluded, workingFolder, save_tmp, last_issue)
            logging.info("Issue-timeline extraction COMPLETE for %s", repoName)
            exception_thrown = False
            break  # Exit the while loop if no exception was thrown
        except KeyboardInterrupt as e:
            # ----- 1. user pressed Ctrl-C --------------------------------
            logging.warning("Extraction interrupted by user – saving progress and exiting.")
            _flush_issues_data_to_csv(issues_df, events_df, comments_df, timeline_df, excluded, workingFolder, save_tmp, last_issue)
            raise e
        # ----- exception handlers unchanged ------------------------
        except (GithubException, AttributeError, Exception) as e:
            logging.warning(f"Extraction interrupted: {e}")
            _flush_issues_data_to_csv(issues_df, events_df, comments_df, timeline_df, excluded, workingFolder, save_tmp, last_issue)
            exception_thrown = True
            raise

        finally:
            _flush_issues_data_to_csv(issues_df, events_df, comments_df, timeline_df, excluded, workingFolder, save_tmp, last_issue)
    
    pbar.close()

    with open(os.path.join(workingFolder, status_tmp), "w") as fh:
        fh.write(f"COMPLETE;{cfg.data_collection_date}")

    return g, token



def _flush_issues_data_to_csv(
        issues_df, events_df, comments_df,
        timeline_df, excluded_df,
        workingFolder, save_tmp, last_issue, page,
        save_last_page, save_current_page, is_page_finished=False):

    _safe_append(issues_df, Path(workingFolder, cfg.issue_list_file_name))
    _safe_append(events_df,  Path(workingFolder, cfg.issue_events_list_file_name))
    _safe_append(comments_df,Path(workingFolder, cfg.issue_comments_list_file_name))
    _safe_append(timeline_df, Path(workingFolder, cfg.issue_timeline_file_name))
    _safe_append(excluded_df, Path(workingFolder, "_excludedNoneType_Issues.tmp"))

    # remember progress
    with open(Path(workingFolder, save_tmp), "w") as fh:
        fh.write(str(last_issue))

    # mirror the commit logic for finished / unfinished pages
    current_page = (
        util.getLastPageRead(Path(workingFolder, save_current_page))
        if Path(workingFolder, save_current_page).exists()
        else 0
    )
    with open(Path(workingFolder, save_last_page), "w") as fh:
        fh.write(
            f"last_page_flushed:{page}" if is_page_finished
            else f"last_page_flushed:{current_page}"
        )



def _safe_append(df, path):
    if df is None or df.empty:
        return
    path = Path(path)
    with path.open("a", encoding="utf-8") as fh:
        portalocker.lock(fh, portalocker.LOCK_EX)   # 1️⃣ block other writers
        df.to_csv(fh,
                  header=fh.tell() == 0,            # 2️⃣ write header only if file was empty
                  sep=cfg.CSV_separator,
                  index=False,
                  lineterminator="\n")
        portalocker.unlock(fh)

def _flush_data_to_csv(commits_df, prs_df, prs_comments_df, excluded, workingFolder, save_last_page, save_current_page, page, is_page_finished = False):
    if commits_df is not None:
        _safe_append(commits_df,  Path(workingFolder, cfg.commit_list_file_name))
    if prs_df is not None:
        _safe_append(prs_df,      Path(workingFolder, cfg.PR_list_file_name))
    if prs_comments_df is not None:
        _safe_append(prs_comments_df,
                 Path(workingFolder, cfg.prs_comments_csv))
    
    _safe_append(excluded,
                 Path(workingFolder, "_excludedNoneType.tmp"))
    
    current_page = (util.getLastPageRead(Path(workingFolder, save_current_page))
                   if Path(workingFolder, save_current_page).exists()
                   else 0)
    
    if is_page_finished:
        with open(os.path.join(workingFolder, save_last_page), "w") as fh:
            fh.write(f"last_page_flushed:{page}")
    else:
        #do nothing if the current page is not larger than the last page
        with open(os.path.join(workingFolder, save_last_page), "w") as fh:
            fh.write(f"last_page_flushed:{current_page}")

        
def updatePageFile(workingFolder, next_page):

    current_page = (util.getLastPageRead(Path(workingFolder, next_page))
                   if Path(workingFolder, next_page).exists()
                   else 0)
    
    new_page = current_page + 1

    with open(os.path.join(workingFolder, next_page), "w") as fh:
        fh.write(f"Next Page:{new_page}")
    return current_page


def get_commit_based_core_devs(commits, threshold=0.8):
    """
    commits: List[dict] where each dict contains at least the 'author' key.
    Example: [{'author': 'alice'}, {'author': 'bob'}, {'author': 'alice'}, ...]

    Returns: List of core developers (author names) who together authored >= threshold of commits.
    """
    # Count commits per developer
    author_commit_counts = Counter(commit["author_id"] for commit in commits)

    # Sort developers by number of commits (descending)
    sorted_authors = author_commit_counts.most_common()

    total_commits = sum(author_commit_counts.values())
    cumulative = 0
    core_devs = []

    for author, count in sorted_authors:
        cumulative += count
        core_devs.append(author)
        if cumulative / total_commits >= threshold:
            break

    return core_devs


### MAIN FUNCTION
def main(gitRepoName):
    ### SET THE PROJECT
    splitRepoName = gitRepoName.split('/')
    organization = splitRepoName[0]
    project = splitRepoName[1]

    organizationsFolder = cfg.main_folder
    os.makedirs(organizationsFolder, exist_ok=True)

    organizationFolder = os.path.join(organizationsFolder, organization)
    os.makedirs(organizationsFolder, exist_ok=True)
    
    full_extraction= True

    runALLExtractionRoutine(organizationFolder, organization, project, full_extraction)
    token = util.getSpisificToken(0)
    g0       = Github(token)
    org = g0.get_organization(organization)

    org_repos = [r for r in org.get_repos(type='sources')
             if not r.archived and not r.fork]

    try: ### Only for Log (Block)
        num_repos = org_repos.totalCount - 1
    except:
        num_repos = 'Unknown'

    full_extraction = False
    repo_num = 0 ### Only for Log
    for repo in org_repos:
        project_name = repo.name
        if project_name != project:
            repo_num += 1 ### Only for Log
            print('Running Commit Extraction for {} ({}/{})'.format(project_name, repo_num, num_repos))
            runALLExtractionRoutine( organizationFolder, organization, project, "Commit" )
     

    

if __name__ == "__main__":
    #add an atribute of id when call the file
    if len(sys.argv) < 1:
        print("Usage: python CommitExtractor.py")
        sys.exit(1)

    THIS_FOLDER = os.path.dirname(os.path.abspath(__file__))
    os.chdir(THIS_FOLDER)

    os.makedirs(cfg.logs_folder, exist_ok=True)
    timestamp = datetime.now().strftime('%Y-%m-%d_%H:%M')
    logfile = cfg.logs_folder+f"/Commit_Extraction-{timestamp}.log"
    logging.basicConfig(filename=logfile, level=logging.INFO)
    
    repoUrls = '../' + cfg.repos_file
    with open(repoUrls) as f:
        repoUrls = f.readlines()
        for repoUrl in repoUrls:
            gitRepoName = repoUrl.replace('https://github.com/', '').strip()
            print('Running Commit Extraction for {}'.format(gitRepoName))
            main(gitRepoName)
            print('Commit Extraction for {} Completed'.format(gitRepoName))
        print('Done.')