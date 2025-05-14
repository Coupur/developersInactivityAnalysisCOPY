### IMPORT EXCEPTION MODULES
from requests.exceptions import Timeout
from github import GithubException
from github.GithubException import IncompletableObject

### IMPORT SYSTEM MODULES
from github import Github
import os, logging, pandas, csv
from datetime import datetime, timezone
from tqdm import tqdm


### IMPORT CUSTOM MODULES
import sys
sys.path.append('../')
import Settings as cfg
import Utilities as util

### DEFINE CONSTANTS
COMPLETE = "COMPLETE"

#import github

def getCommitExtractionStatus(folder, statusFile):
    status = "NOT-STARTED"
    if(statusFile in os.listdir(folder)):
        with open(os.path.join(folder, statusFile)) as f:
            content = f.readline().strip()
        status, _ = content.split(';')
    return status

def runCommitExtractionRoutine(g, token, organizationFolder, organization, project):
    workingFolder = (os.path.join(organizationFolder, project))
    os.makedirs(workingFolder, exist_ok=True)

    logging.info('Commit Extraction for {}'.format(project))

    g, token = util.waitRateLimit(g, token)
    repoName = organization + '/' + project
    repo = g.get_repo(repoName)

    project_start_dt = repo.created_at # To make it a string: stringData=datetimeData.strftime('%Y-%m-%d %H:%M:%S')

    collection_day = datetime.strptime(cfg.data_collection_date, '%Y-%m-%d')

    fileStatus = getCommitExtractionStatus(workingFolder, "_extractionStatus.tmp")
    if (fileStatus != COMPLETE):
        g, token = util.waitRateLimit(g, token)
        g, token = updateCommitListFile(g, token, repoName, project_start_dt, collection_day, workingFolder)
    try:
        commits_data = pandas.read_csv(os.path.join(workingFolder, cfg.commit_list_file_name), sep=cfg.CSV_separator)

        if (cfg.commit_history_table_file_name in os.listdir(workingFolder)):
            commit_table = pandas.read_csv(os.path.join(workingFolder, cfg.commit_history_table_file_name), sep=cfg.CSV_separator)
        else:
            logging.info("Start Writing Commit History Table for {}".format(project))
            commit_table = writeCommitHistoryTable(workingFolder, commits_data)

        logging.info('Starting Inactivity Computation for {}'.format(project))
        writePauses(workingFolder, commit_table)
    except:
        logging.info("No Commits for {}".format(project))
    return g, token

def updateCommitListFile(g, token, repoName, start_date, end_date, workingFolder):
    """Writes the list of the commits fro the given repository"""

    commits_csv = cfg.commit_list_file_name
    prs_csv     = cfg.PR_list_file_name
    prs_comments_csv = cfg.prs_comments_csv
    prs_reviews_csv = cfg.prs_reviews_csv

    save_tmp    = "_saveFile.tmp"
    excl_tmp    = "_excludedNoneType.tmp"
    status_tmp  = "_extractionStatus.tmp"

    status = getCommitExtractionStatus(workingFolder, status_tmp)
    if status == COMPLETE:                     # already done
        return g, token

    os.makedirs(workingFolder, exist_ok=True)
    with open(os.path.join(workingFolder, status_tmp), "w") as fh:
        fh.write(f"INCOMPLETE;{datetime.today():%Y-%m-%d %H:%M:%S}")

    commit_cols = [
        "sha", "PR_id", "author_id", "committer_id", "date",
        "filename_list", "fileschanged_count", "additions_sum", "deletions_sum"
    ]
    pr_cols = [
        "PR_id", "author_id", "state", "merged",
        "created_at", "closed_at", "merged_at",
        "sha_list", "filename_list",
        "fileschanged_count", "additions_sum", "deletions_sum"
    ]

    pr_comments = [
        "PR_id", "comment_id", "review_id", "user_id", "created_at", "body" ]
    
    pr_reviews = [
        "PR_id", "review_id", "user_id", "created_at", "body", ]



    commits_df = (pandas.read_csv(os.path.join(workingFolder, commits_csv), sep=cfg.CSV_separator)
                  if commits_csv in os.listdir(workingFolder)
                  else pandas.DataFrame(columns=commit_cols))

    prs_df = (pandas.read_csv(os.path.join(workingFolder, prs_csv), sep=cfg.CSV_separator)
              if prs_csv in os.listdir(workingFolder)
              else pandas.DataFrame(columns=pr_cols))
    
    prs_comments_df = (pandas.read_csv(os.path.join(workingFolder, prs_comments_csv), sep=cfg.CSV_separator)
                        if prs_comments_csv in os.listdir(workingFolder)
                        else pandas.DataFrame(columns=pr_comments))
    
    prs_reviews_df = (pandas.read_csv(os.path.join(workingFolder, prs_reviews_csv), sep=cfg.CSV_separator)
                        if prs_reviews_csv in os.listdir(workingFolder)
                        else pandas.DataFrame(columns=pr_reviews))

    excluded = (pandas.read_csv(os.path.join(workingFolder, excl_tmp), sep=cfg.CSV_separator)
                if excl_tmp in os.listdir(workingFolder)
                else pandas.DataFrame(columns=["sha"]))

    g, token = util.waitRateLimit(g, token)
    repo     = g.get_repo(repoName)

    # commits total-count (for page math)
    commits   = repo.get_commits(since=start_date, until=end_date)
    last_page = int(commits.totalCount / cfg.items_per_page)
    start_page = util.getLastPageRead(os.path.join(workingFolder, save_tmp)) if save_tmp in os.listdir(workingFolder) else 0
    total_pages = last_page - start_page + 1

    pbar = tqdm(total=total_pages, desc="Downloading", unit="page")

    try:
        for page in range(start_page, last_page + 1):
            
            pbar.update(1)
            pbar.set_postfix({
                "core_left": g.get_rate_limit().core.remaining,
                "repo": repoName.split('/')[-1],
                "page": f"{page}/{last_page}"
            })
            g, token = util.waitRateLimit(g, token)

            start_date = _ensure_utc(start_date)
            end_date   = _ensure_utc(end_date)

            # Pull-requests for this page (GitHub paginates newest-first)
            pulls_page = repo.get_pulls(state="all", sort="created", direction="desc").get_page(page)

            for pr in pulls_page:
                g, token = util.waitRateLimit(g, token)

                pr_id       = pr.number
                pr_author   = pr.user.login if pr.user else None
                pr_state    = pr.state
                pr_merged   = bool(pr.merged)
                pr_created  = pr.created_at
                pr_closed   = pr.closed_at
                pr_mergedat = pr.merged_at

                sha_list, filename_set = [], set()
                add_sum = del_sum = 0

                for pr_commit in pr.get_commits():
                    g, token = util.waitRateLimit(g, token)
                    sha = pr_commit.sha

                    # skip processed / ghost commits
                    if sha in commits_df.sha.values or sha in excluded.sha.values:
                        continue
                    if pr_commit.author is None:
                        util.add(excluded, [sha])
                        continue

                    author_id    = pr_commit.author.login
                    committer_id = pr_commit.commit.committer.name
                    date         = pr_commit.commit.author.date

                    detailed = repo.get_commit(sha)
                    files    = detailed.files
                    filenames = [f.filename for f in files]
                    fchg_cnt = len(filenames)
                    adds      = sum(f.additions for f in files)
                    dels      = sum(f.deletions for f in files)

                    sha_list.extend([sha])
                    filename_set.update(filenames)
                    add_sum += adds
                    del_sum += dels

                    util.add(
                        commits_df,
                        [sha, pr_id, author_id, committer_id, date,
                         "|".join(filenames), fchg_cnt, adds, dels]
                    )
                    util.add(excluded, [sha])

                    for cmt in pr.get_comments():
                        util.add(
                            prs_comments_df,
                            [
                                pr_id,                           # PR_id FK
                                cmt.id,                          # comment_id
                                cmt.pull_request_review_id,      # review_id FK (may be None)
                                cmt.user.login if cmt.user else None,
                                cmt.created_at,
                                (cmt.body or "").replace("\n", " ")
                            ]
                        )
                    
                    for rvw in pr.get_reviews():
                        util.add(
                            prs_reviews_df,
                            [
                                pr_id,                   # PR_id FK
                                rvw.id,                  # review_id
                                rvw.user.login if rvw.user else None,
                                rvw.submitted_at,
                                (rvw.body or "").replace("\n", " ")  # preview text
                            ]
                        )

                # finished one PR row

                util.add(
                    prs_df,
                    [pr_id, pr_author, pr_state, pr_merged,
                     pr_created, pr_closed, pr_mergedat,
                     "|".join(sha_list),
                     "|".join(sorted(filename_set)),
                     len(filename_set), add_sum, del_sum]
                )

    except KeyboardInterrupt:         # Ctrl-C
        logging.warning("Interrupted by user  flushing partial data")
        
        commits_df.to_csv(os.path.join(workingFolder, commits_csv),
                        sep=cfg.CSV_separator, index=False, lineterminator="\n")
        prs_df.to_csv(os.path.join(workingFolder, prs_csv),
                        sep=cfg.CSV_separator, index=False, lineterminator="\n")
        prs_reviews_df.to_csv(os.path.join(workingFolder, prs_reviews_csv),
                        sep=cfg.CSV_separator, index=False, lineterminator="\n")
        prs_comments_df.to_csv(os.path.join(workingFolder, prs_comments_csv),
                        sep=cfg.CSV_separator, index=False, lineterminator="\n")
        excluded.to_csv(os.path.join(workingFolder, excl_tmp),
                        sep=cfg.CSV_separator, index=False, lineterminator="\n")
        
        with open(os.path.join(workingFolder, save_tmp), "w") as fh:
            fh.write(f"last_page:{page}")
        raise
    
    except Exception as e:
        logging.warning(f"Extraction interrupted: {e}") # flush everything we have so far
        commits_df.to_csv(os.path.join(workingFolder, commits_csv),
                          sep=cfg.CSV_separator, index=False, lineterminator="\n")
        prs_df.to_csv(os.path.join(workingFolder, prs_csv),
                      sep=cfg.CSV_separator, index=False, lineterminator="\n")
        excluded.to_csv(os.path.join(workingFolder, excl_tmp),
                        sep=cfg.CSV_separator, index=False, lineterminator="\n")
        with open(os.path.join(workingFolder, save_tmp), "w") as fh:
            fh.write(f"last_page:{page}")
        raise                       # propagate so caller can see it

    finally:
        pbar.close()

    commits_df.to_csv(os.path.join(workingFolder, commits_csv),
                    sep=cfg.CSV_separator, index=False, lineterminator="\n")
    prs_df.to_csv(os.path.join(workingFolder, prs_csv),
                    sep=cfg.CSV_separator, index=False, lineterminator="\n")
    prs_reviews_df.to_csv(os.path.join(workingFolder, prs_reviews_csv),
                    sep=cfg.CSV_separator, index=False, lineterminator="\n")
    prs_comments_df.to_csv(os.path.join(workingFolder, prs_comments_csv),
                    sep=cfg.CSV_separator, index=False, lineterminator="\n")

    if len(excluded):
        excluded.to_csv(os.path.join(workingFolder, excl_tmp),
                        sep=cfg.CSV_separator, index=False, lineterminator="\n")

    with open(os.path.join(workingFolder, status_tmp), "w") as fh:
        fh.write(f"COMPLETE;{cfg.data_collection_date}")

    logging.info("Commit + PR extraction COMPLETE for %s", repoName)
    return g, token


def writeCommitHistoryTable(workingFolder, commits_data):
    """Writes the commit history in form of a table of days x developers. Each cell contains the number of commits"""
    # GET MIN and MAX COMMIT DATETIME
    max_commit_date = max(commits_data['date'])
    min_commit_date = min(commits_data['date'])

    column_names = ['user_id']
    for single_date in util.daterange(min_commit_date, max_commit_date):  # daterange(min_commit_date, max_commit_date):
        column_names.append(single_date.strftime("%Y-%m-%d"))

    # ITERATE UNIQUE USERS (U)
    devs_list = commits_data.author_id.unique()
    u_data = []
    for u in devs_list:
        user_id = u
        cur_user_data = [user_id]
        date_commit_count = pandas.to_datetime(commits_data[['date']][commits_data['author_id'] == u].pop('date'),
                                               format="%Y-%m-%d %H:%M:%S%z").dt.date.value_counts()
        # ITERATE FROM DAY1 --> DAYN (D)
        for d in column_names[1:]:
            # IF U COMMITTED DURING D THEN U[D]=1 ELSE U(D)=0
            try:
                cur_user_data.append(date_commit_count[pandas.to_datetime(d).date()])
            except Exception:  # add "as name_given_to_exception" before ":" to get info
                cur_user_data.append(0)
        # print("finished user", u)
        u_data.append(cur_user_data)

    commit_table = pandas.DataFrame(u_data, columns=column_names)
    commit_table.to_csv(os.path.join(workingFolder, cfg.commit_history_table_file_name),
                        sep=cfg.CSV_separator, na_rep=cfg.CSV_missing, index=False, quoting=None, lineterminator='\n')
    logging.info("Commit History Table Written")
    return commit_table

def writePauses(workingFolder, commit_table):
    """Computes the Pauses and writes
    1. the Intervals file containing for each developer the list of its pauses' length
    2. the Breaks Dates file containing for each developer the list of date intervals"""
    # Calcola days between commits, if commits are in adjacent days count 1
    pauses_duration_list = []
    pauses_dates_list = []
    for _, u in commit_table.iterrows():
        row = [u.iloc[0]] #[u[0]]  # User_id
        current_pause_dates = [u.iloc[0]] # [u[0]]  # User_id
        commit_dates = []
        for i in range(1, len(u)):
            if (u.iloc[i] > 0): # if (u[i] > 0):
                commit_dates.append(commit_table.columns[i])
        for i in range(0, len(commit_dates) - 1):
            period = util.daysBetween(commit_dates[i], commit_dates[i + 1])
            if (period > 1):
                row.append(period)
                current_pause_dates.append(commit_dates[i] + '/' + commit_dates[i + 1])
        # ADD LAST PAUSE
        last_commit_day = commit_dates[-1]
        collection_day=cfg.data_collection_date
        period = util.daysBetween(last_commit_day, collection_day)
        if (period > 1):
            row.append(period)
            current_pause_dates.append(last_commit_day + '/' + collection_day)

        # Wrap up the list
        pauses_duration_list.append(row)
        pauses_dates_list.append(current_pause_dates)
        user_lifespan = util.daysBetween(commit_dates[0], commit_dates[len(commit_dates) - 1]) + 1
        commit_frequency = len(commit_dates) / user_lifespan
        row.append(user_lifespan)
        row.append(commit_frequency)
    logging.info('Inactivity Computation Done')

    with open(os.path.join(workingFolder, cfg.pauses_list_file_name), 'w', newline='') as outcsv:
        # configure writer to write standard csv file
        writer = csv.writer(outcsv, quoting=csv.QUOTE_NONE, delimiter=cfg.CSV_separator, quotechar='"', escapechar='\\')
        for r in pauses_duration_list:
            # Write item to outcsv
            writer.writerow(r)

    with open(os.path.join(workingFolder, cfg.pauses_dates_file_name), 'w', newline='') as outcsv:
        # configure writer to write standard csv file
        writer = csv.writer(outcsv, quoting=csv.QUOTE_NONE, delimiter=cfg.CSV_separator, quotechar='"', escapechar='\\')
        for r in pauses_dates_list:
            # Write item to outcsv
            writer.writerow(r)

def mergeProjectsCommits(path, main_project_name):  # No filter on core_devs_df. All developers are taken
    proj_path = os.path.join(path, main_project_name)
    commits_data = pandas.read_csv(os.path.join(proj_path, cfg.commit_list_file_name), sep=cfg.CSV_separator)

    projects = os.listdir(path)
    for project in projects:
        if ((project != main_project_name) and (os.path.isdir(os.path.join(path, project)))):
            proj_path = os.path.join(path, project)
            if cfg.commit_list_file_name in os.listdir(proj_path):
                project_commits = pandas.read_csv(os.path.join(proj_path, cfg.commit_list_file_name), sep=cfg.CSV_separator)
                commits_data = pandas.concat([commits_data, project_commits], ignore_index=True)

    commits_data.to_csv(os.path.join(path, cfg.commit_list_file_name),
                        sep=cfg.CSV_separator, na_rep=cfg.CSV_missing, index=False, quoting=None, lineterminator='\n')
    logging.info('All the Organization commits have been merged with '.format(main_project_name))

def _ensure_utc(dt):
    """
    Return the same datetime as an *offset-aware* object in UTC.
    Safe to call on values that are already aware.
    """
    if dt.tzinfo is None:
        return dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc)


### MAIN FUNCTION
def main(gitRepoName, token):
    ### SET THE PROJECT
    splitRepoName = gitRepoName.split('/')
    organization = splitRepoName[0]
    project = splitRepoName[1]

    g = Github(token)
    try:
        g.get_rate_limit()
    except Exception as e:
        print('Exception {}'.format(e))
        logging.error(e)
        return
    g.per_page = cfg.items_per_page

    print("Running the Commit Extraction. \n Connection Done. \n Logging in {}".format(logfile))
    logging.info("Commit Extraction Started for {}.".format(organization))

    organizationsFolder = cfg.main_folder
    os.makedirs(organizationsFolder, exist_ok=True)

    organizationFolder = os.path.join(organizationsFolder, organization)
    os.makedirs(organizationsFolder, exist_ok=True)

    g, token = runCommitExtractionRoutine(g, token, organizationFolder, organization, project)
    #core_devs_df = findCoreDevelopers(organizationFolder, project)
    #core_devs_list = core_devs_df.dev.tolist()
    #logging.info('Commit Extraction COMPLETE for the Main Project: {}! {} Core developers found.'.format(project, len(core_devs_list)))

    g, token = util.waitRateLimit(g, token)
    org = g.get_organization(organization)
    org_repos = org.get_repos(type='all')

    try: ### Only for Log (Block)
        num_repos = org_repos.totalCount - 1
    except:
        num_repos = 'Unknown'

    repo_num = 0 ### Only for Log
    for repo in org_repos:
        g, token = util.waitRateLimit(g, token)
        project_name = repo.name
        if project_name != project:
            repo_num += 1 ### Only for Log
            g, token = runCommitExtractionRoutine(g, token, organizationFolder, organization, project_name)
    logging.info('Commit Extraction COMPLETE for {} of {} Side Projects'.format(repo_num,num_repos))

    logging.info('Merging the Commits in the WHOLE {} Organization'.format(organization))
    mergeProjectsCommits(organizationFolder, project)  # No filter on core_devs_df. All developers are taken

    if cfg.commit_list_file_name in os.listdir(organizationFolder):
        commits_data = pandas.read_csv(os.path.join(organizationFolder, cfg.commit_list_file_name), sep=cfg.CSV_separator)
        writeCommitHistoryTable(organizationFolder, commits_data)

    if cfg.commit_history_table_file_name in os.listdir(organizationFolder):
        commit_table = pandas.read_csv(os.path.join(organizationFolder, cfg.commit_history_table_file_name), sep=cfg.CSV_separator)
        writePauses(organizationFolder, commit_table)

    logging.info('Commit Extraction SUCCESSFULLY COMPLETED')

if __name__ == "__main__":
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
            token = util.getRandomToken()
            print('Running Commit Extraction for {}'.format(gitRepoName))
            main(gitRepoName, token)
            print('Commit Extraction for {} Completed'.format(gitRepoName))
        print('Done.')