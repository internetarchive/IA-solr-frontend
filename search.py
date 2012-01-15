from flask import Flask, render_template, request
from urllib import urlopen, quote_plus
import json, locale, sys, re

from socket import socket, AF_INET, SOCK_DGRAM, SOL_UDP, SO_BROADCAST, timeout
from time import time

app = Flask(__name__)

re_loc = re.compile('^(ia\d+\.us\.archive\.org):(/\d+/items/(.*))$')

class FindItemError(Exception):
    pass

def find_item(ia):
    s = socket(AF_INET, SOCK_DGRAM, SOL_UDP)
    s.setblocking(1)
    s.settimeout(2.0)
    s.setsockopt(1, SO_BROADCAST, 1)
    s.sendto(ia, ('<broadcast>', 8010))
    for attempt in range(5):
        try:
            (loc, address) = s.recvfrom(1024)
        except timeout:
            raise
        m = re_loc.match(loc)
        if not m:
            continue

        ia_host = m.group(1)
        ia_path = m.group(2)
        if m.group(3) == ia:
            return (ia_host, ia_path)
    raise FindItemError

facet_fields = ['noindex', 'mediatype', 'collection', 'language_facet', 'subject_facet', 'publisher_facet', 'rating', 'sponsor_facet', 'camera', 'handwritten']
year_gap = 10

results_per_page = 30 

def quote(s):
    return quote_plus(s.encode('utf-8')) if not isinstance(s, int) else s

addr = 'ol-search-inside:8984'
solr_select_url = 'http://' + addr + '/solr/select?wt=json' + \
    '&json.nl=arrarr' + \
    '&defType=edismax' + \
    '&qf=text' + \
    '&fl=identifier,creator,title,date,subject,collection,scanner,mediatype,description,noindex,score,case-name,rating,sponsor' + \
    '&spellcheck=true' + \
    '&spellcheck.onlyMorePopular=false' + \
    '&spellcheck.extendedResults=true' + \
    '&spellcheck.count=1' + \
    '&rows=' + str(results_per_page) + \
    '&facet=true&facet.limit=30&facet.mincount=1' + \
    '&facet.range=date&facet.range.start=0000-01-01T00:00:00Z&facet.range.end=2015-01-01T00:00:00Z&facet.range.gap=%2B' + str(year_gap) + 'YEAR' + \
    '&hl=true&hl.fragsize=0&hl.fl=title,creator,subject,collection,description,case-name&hl.simple.pre=' + quote('{{{') + '&hl.simple.post=' + quote('}}}') + \
    '&bq=(*:* -collection:ourmedia -collection:opensource* collection:[* TO *])^10' + \
    '&q.op=AND'
#    '&debugQuery=true' + \
#    '&spellcheck.collate=true' + \
#    '&spellcheck.maxCollationTries=5' + \
#    '&spellcheck.accuracy=0.5' + \

lang_map = {
    'eng': 'English',
    'fre': 'French',
    'ger': 'German',
    'spa': 'Spanish',
    'ita': 'Italian',
    'lat': 'Latin',
    'rus': 'Russian',
    'dut': 'Dutch',
    'ara': 'Arabic',
}

def test_quote():
    assert quote('x') == 'x'
    assert quote('x x') == 'x+x'
    assert quote('@test test@') == '%40test+test%40'

locale.setlocale(locale.LC_ALL, 'en_US')

def comma(n):
    return locale.format("%d", n, grouping=True)

def test_comma():
    assert comma(1000) == '1,000'
    assert comma(-5) == '-5'
    assert comma(-5000) == '-5,000'

def pick_best(thumbs, num=4, start_skip=3):
    if len(thumbs) <= num:
        return thumbs
    if len(thumbs) < num + 3:
        return thumbs[1:1+num]
    if len(thumbs) == num + 3:
        return thumbs[1:1+num]
    return [v for i, v in list(enumerate(thumbs))[start_skip:] if i % ((len(thumbs)-start_skip)/num) == 0][:num]

def test_pick_best():
    assert pick_best(range(20)) == [4, 8, 12, 16]
    assert pick_best(range(4)) == range(4)
    assert pick_best(range(5)) == [1,2,3,4]
    assert pick_best(range(6)) == [1,2,3,4]
    assert pick_best(range(7)) == [1,2,3,4]
    assert pick_best(range(8)) == [3, 4, 5, 6]
    assert pick_best(range(9)) == [3, 4, 5, 6]

def token_hl(s):
    while '{{{' in s:
        start = s.find('{{{')
        end = s.find('}}}', start)
        if start:
            yield ('text', s[:start])
        yield ('hl', s[start+3:end])
        s = s[end+3:]
    if s:
        yield ('text', s)

def test_token_hl():
    l = list(token_hl('abc'))
    assert l == [('text', 'abc')]
    l = list(token_hl('{{{aaa}}} bbb'))
    assert l == [('hl', 'aaa'), ('text', ' bbb')]
    l = list(token_hl('aaa {{{bbb}}}'))
    assert l == [('text', 'aaa '), ('hl', 'bbb')]
    l = list(token_hl('aaa {{{bbb}}} ccc'))
    assert l == [('text', 'aaa '), ('hl', 'bbb'), ('text', ' ccc')]

re_thumb_dir_link = re.compile('<a href="(.+\.thumbs/)">')
re_link = re.compile('<a href="(.+)">')
def get_movie_thumb(identifier):
    try:
        host, path = find_item(identifier)
    except FindItemError:
        return
    for line in urlopen('http://' + host + path):
        m = re_thumb_dir_link.match(line)
        if m:
            thumb_dir = m.group(1)
            break
    else:
        return
    thumbs = []
    for line in urlopen('http://' + host + path + '/' + thumb_dir):
        m = re_link.match(line)
        if m:
            thumbs.append(m.group(1))
    return {
        'url': 'http://' + host + path + '/' + thumb_dir + '/',
        'imgs': thumbs,
    }

re_thumb_link = re.compile('<a href="(.+thumb.*)">')

def changequery(new_args):
    args = dict((k, v) for k, v in request.args.items() if v and k not in new_args)
    args.update(dict((k, v) for k, v in new_args.items() if v is not None))
    return '&'.join('%s=%s' % (k, quote(v)) for k, v in args.items())

def test_changequery():
    with app.test_client() as c:
        rv = c.get('/?mediatype=movies&language=eng')
        assert request.args['q'] == 'test'
        assert request.args['mediatype'] == 'movies'
        assert request.args['language'] == 'eng'
        url = changequery({'collection':'nasa'})
        assert url == 'q=test&mediatype=movies&language=eng&collection=nasa'
        url = changequery({'language':None})
        assert url == 'q=test&mediatype=movies'

def get_img_thumb(identifier):
    try:
        host, path = find_item(identifier)
    except FindItemError:
        return
    for line in urlopen('http://' + host + path):
        m = re_thumb_link.match(line)
        if m:
            return 'http://' + host + path + '/' + m.group(1)

re_date_range = re.compile('^(\d+)-(\d+)$')

def build_pager(num_found, page, pages_in_set = 10):
    pages = (num_found / results_per_page) + 1
    half = pages_in_set/2
    if pages < pages_in_set:
        first_page_in_set = 1
        last_page_in_set = pages
    if page < half:
        first_page_in_set = 1
        last_page_in_set = min((pages_in_set, pages))
    elif page > (pages-half):
        first_page_in_set = max((pages-pages_in_set, 1))
        last_page_in_set = pages
    else:
        first_page_in_set = max((page-half, 1))
        last_page_in_set = page+half
    return {
        'pages': pages,
        'page_list': range(first_page_in_set, last_page_in_set+1),
    }

def get_collection_titles(results):
    collections = set(key for key, num in results['facet_counts']['facet_fields']['collection'])
    for doc in results['response']['docs']:
        collections.update(doc['collection'])

    if not collections:
        return {}

    url = 'http://' + addr + '/solr/select?wt=json' + \
        '&json.nl=arrarr' + \
        '&defType=edismax' + \
        '&qf=identifier' + \
        '&q=' + quote(' OR '.join(collections)) + \
        '&fl=identifier,title' + \
        '&rows=%d' % len(collections)
    ret = urlopen(url).read()
    try:
        data = json.loads(ret)
    except ValueError:
        print ret
        raise
    return dict((c['identifier'], c['title']) for c in data['response']['docs'])

@app.route("/")
def do_search():
    q = request.args.get('q')
    if not q:
        return render_template('search.html')
    facet_args = [(f, request.args[f]) for f in facet_fields if f in request.args]
    facet_args_dict = dict(facet_args)
    quote_q = quote(q)
    page = int(request.args.get('page', 1))
    start = results_per_page * (page-1)
    #fq = ''.join('&fq=' + quote('{!tag=%s}{!term f=%s}%s' % (f, f, request.args[f])) for f in facet_fields if f in request.args)
    fq = ''.join('&fq=' + quote('{!tag=%s}%s:"%s"' % (f, f, request.args[f])) for f in facet_fields if f in request.args)
    date_range = request.args.get('date_range')
    date_facet = request.args.get('date_facet')
    if date_range:
        m = re_date_range.match(date_range)
        if m:
            start_year, end_year = m.groups()
            fq += '&fq=' + quote('date:[%s-01-01T00:00:00Z TO %s-01-01T00:00:00Z]' % (start_year, end_year))
    elif date_facet:
        fq += '&fq=' + quote('date:([%s-01-01T00:00:00Z TO %s-01-01T00:00:00Z+%dYEAR] NOT "%s-01-01T00:00:00Z+%dYEAR")' % (date_facet, date_facet, year_gap, date_facet, year_gap))

    url = solr_select_url + '&q=' + quote(q) + '&start=%d' % start + fq + ''.join('&facet.field='+('{!ex=' + f + '}' if f in facet_args_dict else '') + f for f in facet_fields)
    t0_solr = time()
    f = urlopen(url)
    reply = f.read()
    t_solr = time() - t0_solr
    try:
        results = json.loads(reply)
    except ValueError:
        return reply

    collection_titles = get_collection_titles(results)

    pager = build_pager(results['response']['numFound'], page)

    return render_template('search.html', q=q, page=page, results=results, \
            results_per_page=results_per_page, pager=pager, \
            quote=quote, comma=comma, int=int, facet_fields=facet_fields, lang_map=lang_map, facet_args=facet_args, \
            get_movie_thumb=get_movie_thumb, year_gap=year_gap, find_item=find_item, enumerate=enumerate, len=len, pick_best=pick_best, url=url, \
            facet_args_dict=facet_args_dict,\
            get_img_thumb = get_img_thumb, changequery=changequery, token_hl=token_hl, t_solr=t_solr, collection_titles=collection_titles)
    
if __name__ == "__main__":
    app.run(host='0.0.0.0', port=8081, debug=True)
