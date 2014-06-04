from boto import connect_s3
from lxml import html
from lxml.etree import ParserError
from pygraph.classes.digraph import digraph
from pygraph.algorithms.pagerank import pagerank
from pygraph.classes.exceptions import AdditionError
from wikia_authority import MinMaxScaler
import logging
import traceback
import json
import requests
import sys
import multiprocessing
import argparse
import time


minimum_authors = 5
minimum_contribution_pct = 0.05
smoothing = 0.05
wiki_id = None
api_url = None
edit_distance_memoization_cache = {}

log = logging.getLogger(__name__)
log.setLevel(logging.DEBUG)
log.addHandler(logging.StreamHandler())
fh = logging.FileHandler('api_to_database.log')
fh.setLevel(logging.ERROR)
log.addHandler(fh)


class Unbuffered:

    def __init__(self, stream):
        self.stream = stream

    def write(self, data):
        self.stream.write(data)
        self.stream.flush()

    def __getattr__(self, attr):
        return getattr(self.stream, attr)

sys.stdout = Unbuffered(sys.stdout)


# multiprocessing's gotta grow up and let me do anonymous functions
def set_page_key(x):
    log.debug(u"Setting page key for %s" % x)
    bucket = connect_s3().get_bucket(u'nlp-data')
    k = bucket.new_key(
        key_name=u'/service_responses/%s/PageAuthorityService.get' % (
            x[0].replace(u'_', u'/')))
    k.set_contents_from_string(json.dumps(x[1], ensure_ascii=False))
    return True


def get_all_titles(aplimit=500):
    global api_url, wiki_id
    params = {u'action': u'query', u'list': u'allpages', u'aplimit': aplimit,
              u'apfilterredir': u'nonredirects', u'format': u'json'}
    allpages = []
    while True:
        resp = requests.get(api_url, params=params)
        try:
            response = resp.json()
        except ValueError:
            log.error(u"%s\n%s" % (wiki_id, traceback.format_exc()))
            log.error(resp.content)
            return allpages
        resp.close()
        allpages += response.get(u'query', {}).get(u'allpages', [])
        if u'query-continue' in response:
            params[u'apfrom'] = response[u'query-continue'][u'allpages'][
                u'apfrom']
        else:
            break
    return allpages


def get_all_revisions(title_object):
    global api_url, wiki_id
    title_string = title_object[u'title']
    params = {u'action': u'query',
              u'prop': u'revisions',
              u'titles': title_string.encode(u'utf8'),
              u'rvprop': u'ids|user|userid',
              u'rvlimit': u'max',
              u'rvdir': u'newer',
              u'format': u'json'}
    revisions = []
    while True:
        resp = requests.get(api_url, params=params)
        try:
            response = resp.json()
        except ValueError:
            log.error(u"%s\n%s" % (wiki_id, traceback.format_exc()))
            log.error(resp.content)
            return [title_string, revisions]
        resp.close()
        revisions += response.get(u'query', {}).get(
            u'pages', {0: {}}).values()[0].get(u'revisions', [])
        if u'query-continue' in response:
            params[u'rvstartid'] = response[u'query-continue'][u'revisions'][
                u'rvstartid']
            log.debug("Start ID is %s" % params[u'rvstartid'])
        else:
            break
    return [title_string, revisions]


def edit_distance(title_object, earlier_revision, later_revision,
                  already_retried=False):
    global api_url, edit_distance_memoization_cache, wiki_id
    if (earlier_revision, later_revision) in edit_distance_memoization_cache:
        return edit_distance_memoization_cache[(earlier_revision,
                                                later_revision)]
    params = {u'action': u'query',
              u'prop': u'revisions',
              u'rvprop': u'ids|user|userid',
              u'rvlimit': u'1',
              u'format': u'json',
              u'rvstartid': earlier_revision,
              u'rvdiffto': later_revision,
              u'titles': title_object[u'title']}

    try:
        resp = requests.get(api_url, params=params)
    except requests.exceptions.ConnectionError as e:
        log.error(u"%s\n%s" % (wiki_id, traceback.format_exc()))
        log.error(u"Already retried? %s" % str(already_retried))
        if already_retried:
            log.error(u"Gave up on some socket shit %s" % e)
            return 0
        log.error(u"Fucking sockets")
        ## wait 4 minutes for your wimpy ass sockets to get their shit together
        #time.sleep(240)

        # Only wait 10 seconds for debugging purposes
        time.sleep(10)
        return edit_distance(title_object, earlier_revision, later_revision,
                             already_retried=True)

    try:
        response = resp.json()
    except ValueError:
        log.error(u"%s\n%s" % (wiki_id, traceback.format_exc()))
        log.error(resp.content)
        return 0
    resp.close()
    time.sleep(0.025)  # prophylactic throttling
    revision = response.get(u'query', {}).get(u'pages', {0: {}}).get(
        unicode(title_object[u'pageid']), {}).get(u'revisions', [{}])[0]
    revision[u'adds'], revision[u'deletes'], revision[u'moves'] = 0, 0, 0
    if (u'diff' in revision and u'*' in revision[u'diff']
       and revision[u'diff'][u'*'] != '' and revision[u'diff'][u'*'] is not
       False and revision[u'diff'][u'*'] is not None):
        try:
            diff_dom = html.fromstring(revision[u'diff'][u'*'])
            deleted = [word for span in diff_dom.cssselect(
                u'td.diff-deletedline span.diffchange-inline') for word in
                span.text_content().split(' ')]
            added = [word for span in diff_dom.cssselect(
                u'td.diff-addedline span.diffchange-inline') for word in
                span.text_content().split(' ')]
            adds = sum([1 for word in added if word not in deleted])
            deletes = sum([1 for word in deleted if word not in added])
            moves = sum([1 for word in added if word in deleted])
            # bad approx. of % of document
            changes = revision[u'adds']+revision[u'deletes']+revision[u'moves']
            if changes > 0:
                moves /= changes
            distance = max([adds, deletes]) - 0.5*min([adds, deletes]) + moves
            edit_distance_memoization_cache[(earlier_revision,
                                             later_revision)] = distance
            return distance
        except (TypeError, ParserError, UnicodeEncodeError):
            log.error(u"%s\n%s" % (wiki_id, traceback.format_exc()))
            return 0
    return 0


def edit_quality(title_object, revision_i, revision_j):

    numerator = (
        edit_distance(
            title_object, revision_i[u'parentid'], revision_j[u'revid']) -
        edit_distance(
            title_object, revision_i[u'revid'], revision_j[u'revid'])
        )

    denominator = edit_distance(
        title_object, revision_i[u'parentid'], revision_i[u'revid'])

    val = numerator if denominator == 0 or numerator == 0 else (numerator /
                                                                denominator)
    return -1 if val < 0 else 1  # must be one of[-1, 1]


def get_contributing_authors_safe(arg_tuple):
    global wiki_id
    try:
        res = get_contributing_authors(arg_tuple)
    except Exception:
        log.error(u"%s\n%s" % (wiki_id, traceback.format_exc()))
        return str(wiki_id) + '_' + str(arg_tuple[0][u'pageid']), []
    return res


def get_contributing_authors(arg_tuple):
    global minimum_authors, minimum_contribution_pct, smoothing, wiki_id

    #  within scope of map_async subprocess
    requests.Session().mount(
        u'http://',
        requests.adapters.HTTPAdapter(
            pool_connections=1, pool_maxsize=1, pool_block=True)
        )

    title_object, title_revs = arg_tuple
    doc_id = "%s_%s" % (str(wiki_id), title_object[u'pageid'])
    log.debug("Getting contributing authors for %s" % doc_id)
    top_authors = []
    if len(title_revs) == 1 and u'user' in title_revs[0]:
        return doc_id, []
        # will this fix the bug?
        title_revs[0][u'contrib_pct'] = 1
        title_revs[0][u'contribs'] = 1
        return doc_id, title_revs

    for i in range(0, len(title_revs)):
        curr_rev = title_revs[i]
        if i == 0:
            edit_dist = 1
        else:
            prev_rev = title_revs[i-1]
            if u'revid' not in curr_rev or u'revid' not in prev_rev:
                continue
            edit_dist = edit_distance(
                title_object, prev_rev[u'revid'], curr_rev[u'revid'])

        non_author_revs_comps = [
            (title_revs[j-1], title_revs[j]) for j in
            range(i+1, len(title_revs[i+1:i+11])) if
            title_revs[j].get(u'user', u'') != curr_rev.get(u'user')
            ]

        avg_edit_qty = (
            sum(map(lambda x: edit_quality(title_object, x[0], x[1]),
                    non_author_revs_comps)) /
            max(1, len(set([
                non_author_rev_cmp[1].get(u'user', u'') for non_author_rev_cmp
                in non_author_revs_comps])))
            )
        if avg_edit_qty == 0:
            avg_edit_qty = smoothing
        curr_rev[u'edit_longevity'] = avg_edit_qty * edit_dist

    authors = filter(lambda x: x[u'userid'] != 0 and x[u'user'] != u'',
                     dict([(title_rev.get(u'userid', 0),
                            {u'userid': title_rev.get(u'userid', 0),
                             u'user': title_rev.get(u'user', u'')})
                           for title_rev in title_revs]).values())

    for author in authors:
        author[u'contribs'] = sum(
            [title_rev[u'edit_longevity'] for title_rev in title_revs if
             title_rev.get(u'userid', 0) == author.get(u'userid', 0) and
             u'edit_longevity' in title_rev and
             title_rev[u'edit_longevity'] > 0])

    authors = filter(lambda x: x.get(u'contribs', 0) > 0, authors)

    all_contribs_sum = sum([a[u'contribs'] for a in authors])

    for author in authors:
        author[u'contrib_pct'] = author[u'contribs']/all_contribs_sum

    for author in sorted(authors, key=lambda x: x[u'contrib_pct'],
                         reverse=True):
        if u'user' not in author:
            continue
        if author[u'contrib_pct'] < minimum_contribution_pct and (
                len(top_authors) >= minimum_authors):
            break
        top_authors += [author]
    return doc_id, top_authors


def links_for_page(title_object):
    global api_url, wiki_id
    title_string = title_object[u'title']
    params = {
        u'action': u'query', u'titles': title_string.encode(u'utf8'),
        u'plnamespace': 0, u'prop': u'links', u'pllimit': 500,
        u'format': u'json'}
    links = []
    while True:
        resp = requests.get(api_url, params=params)
        try:
            response = resp.json()
        except ValueError:
            log.error(u"%s\n%s" % (wiki_id, traceback.format_exc()))
            log.error(resp.content)
            return links
        resp.close()
        response_links = response.get(u'query', {}).get(
            u'pages', {0: {}}).values()[0].get(u'links', [])
        links += [link[u'title'] for link in response_links]
        query_continue = response.get(u'query-continue', {}).get(
            u'links', {}).get(u'plcontinue')
        if query_continue is not None:
            params[u'plcontinue'] = query_continue
        else:
            break
    return title_string, links


def get_pagerank(args, all_titles):
    pool = multiprocessing.Pool(processes=args.processes)
    r = pool.map_async(links_for_page, all_titles)
    r.wait()
    all_links = r.get()
    all_title_strings = list(set(
        [to_string for response in all_links for to_string in response[1]] +
        [obj[u'title'] for obj in all_titles]))

    wiki_graph = digraph()
    # to prevent missing node_neighbors table
    wiki_graph.add_nodes(all_title_strings)
    for title_object in all_titles:
        for target in links_for_page(title_object)[1]:
            try:
                wiki_graph.add_edge((title_object[u'title'], target))
            except AdditionError:
                pass

    return pagerank(wiki_graph)


def author_centrality(titles_to_authors):
    author_graph = digraph()
    author_graph.add_nodes(map(lambda x: u"title_%s" % x,
                               titles_to_authors.keys()))
    author_graph.add_nodes(list(set(
        [u'author_%s' % author[u'user'] for authors in
         titles_to_authors.values() for author in authors])))

    for title in titles_to_authors:
        log.debug(u"Working on title: %s" % title)
        for author in titles_to_authors[title]:
            try:
                author_graph.add_edge(
                    (u'title_%s' % title, u'author_%s' % author[u'user']))
            except AdditionError:
                pass

    centralities = dict([
        ('_'.join(item[0].split('_')[1:]), item[1]) for item in
        pagerank(author_graph).items() if item[0].startswith(u'author_')])

    centrality_scaler = MinMaxScaler(centralities.values())

    return dict([(cent_author, centrality_scaler.scale(cent_val))
                 for cent_author, cent_val in centralities.items()])


def get_title_top_authors(args, all_titles, all_revisions):
    pool = multiprocessing.Pool(processes=args.processes)
    title_top_authors = {}
    r = pool.map_async(
        get_contributing_authors_safe,
        [(title_obj, all_revisions.get(title_obj[u'title'], [])) for title_obj
         in all_titles],
        callback=title_top_authors.update)
    r.wait()
    if len(title_top_authors) == 0:
        log.info(u"No title top authors for wiki %s" % args.wiki_id)
        log.info(r.get())
        sys.exit(1)
    contribs = [author[u'contribs'] for title in title_top_authors for author
                in title_top_authors[title]]
    if len(contribs) == 0:
        log.info(u"No contributions for wiki %s" % args.wiki_id)
        sys.exit(1)
    contribs_scaler = MinMaxScaler(contribs)
    scaled_title_top_authors = {}
    for title, authors in title_top_authors.items():
        new_authors = []
        for author in authors:
            author[u'contribs'] = contribs_scaler.scale(author[u'contribs'])
            new_authors.append(author)
        scaled_title_top_authors[title] = new_authors
    return scaled_title_top_authors


def get_pagerank_dict(all_titles):
    title_to_pageid = dict(
        [(title_object[u'title'], title_object[u'pageid']) for title_object in
         all_titles])
    pr = dict(
        [(u'%s_%s' % (str(wiki_id), title_to_pageid[title]), pagerank) for
         title, pagerank in get_pagerank(all_titles).items() if title in
         title_to_pageid])
    return pr


def get_args():
    try:
        default_cpus = multiprocessing.cpu_count()
    except NotImplementedError:
        default_cpus = 2   # arbitrary default

    parser = argparse.ArgumentParser(
        description=u'Get authoritativeness data for a given wiki.')
    parser.add_argument(
        u'--wiki-id', dest=u'wiki_id', action=u'store', required=True,
        help=u'The ID of the wiki you want to operate on')
    parser.add_argument(
        u'--processes', dest=u'processes', action=u'store', type=int,
        default=default_cpus,
        help=u'Number of processes you want to run at once')
    return parser.parse_args()


def main():
    global minimum_authors, minimum_contribution_pct, smoothing, wiki_id
    global api_url, edit_distance_memoization_cache

    args = get_args()

    edit_distance_memoization_cache = {}

    smoothing = 0.001

    start = time.time()

    wiki_id = args.wiki_id
    log.info(u"wiki id is %s" % wiki_id)

    minimum_authors = 5
    minimum_contribution_pct = 0.01

    # get wiki info
    resp = requests.get(
        u'http://www.wikia.com/api/v1/Wikis/Details',
        params={u'ids': wiki_id})
    items = resp.json()['items']
    if wiki_id not in items:
        log.info(u"Wiki doesn't exist?")
        sys.exit(1)
    wiki_data = items[wiki_id]
    resp.close()
    log.info(wiki_data[u'title'].encode(u'utf8'))
    api_url = u'%sapi.php' % wiki_data[u'url']

    # can't be parallelized since it's an enum
    all_titles = get_all_titles()
    log.info(u"Got %d titles" % len(all_titles))

    pool = multiprocessing.Pool(processes=args.processes)

    all_revisions = []
    r = pool.map_async(
        get_all_revisions, all_titles, callback=all_revisions.extend)
    r.wait()
    log.info(u"%d Revisions" % sum(
        [len(revs) for title, revs in all_revisions]))
    all_revisions = dict(all_revisions)

    title_top_authors = get_title_top_authors(args, all_titles, all_revisions)

    log.info(time.time() - start)

    centralities = author_centrality(title_top_authors)

    # this com_qscore_pr, the best metric per Qin and Cunningham
    comqscore_authority = dict([(
        doc_id,
        sum([author[u'contribs'] * centralities[author[u'user']] for author in
             authors])
        ) for doc_id, authors in title_top_authors.items()])

    log.info(u"Got comsqscore, storing data")

    bucket = connect_s3().get_bucket(u'nlp-data')
    key = bucket.new_key(
        key_name=u'service_responses/%s/WikiAuthorCentralityService.get' % (
            wiki_id))
    key.set_contents_from_string(json.dumps(centralities, ensure_ascii=False))

    key = bucket.new_key(
        key_name=u'service_responses/%s/WikiAuthorityService.get' % wiki_id)
    key.set_contents_from_string(
        json.dumps(comqscore_authority, ensure_ascii=False))

    q = pool.map_async(
        set_page_key,
        title_top_authors.items()
    )
    q.wait()

    log.info(u"%s finished in %s seconds" % (wiki_id, (time.time() - start)))


if __name__ == u'__main__':
    try:
        main()
    except Exception:
        wiki_id = get_args().wiki_id
        log.error(u"%s\n%s" % (wiki_id, traceback.format_exc()))
