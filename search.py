from flask import Flask, render_template, request
from urllib import urlopen, quote_plus
import json, locale, sys, re

from socket import socket, AF_INET, SOCK_DGRAM, SOL_UDP, SO_BROADCAST, timeout
import re

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

facet_fields = ['noindex', 'mediatype', 'collection', 'language_facet', 'subject_facet', 'publisher_facet']
year_gap = 10

results_per_page = 20 

solr_select_url = 'http://localhost:8983/solr/select?wt=json' + \
    '&json.nl=arrarr' + \
    '&defType=edismax' + \
    '&fl=identifier,title,date,subject,collection,scanner,mediatype,description,noindex,score' + \
    '&rows=' + str(results_per_page) + \
    '&facet=true&facet.limit=30&facet.mincount=2' + \
    '&facet.range=date&facet.range.start=0000-01-01T00:00:00Z&facet.range.end=2015-01-01T00:00:00Z&facet.range.gap=%2B' + str(year_gap) + 'YEAR' + \
    '&hl=true&hl.fl=title,subject,collection,description' + \
    '&bq=(*:* -collection:ourmedia -collection:opensource*)^10' + \
    '&q.op=AND'

solr_spell_url = 'http://localhost:8983/solr/spell?wt=json&spellcheck=true&spellcheck.collate=true&spellcheck.extendedResults=true&spellcheck.maxCollations=3&spellcheck.maxCollationTries=3&rows=0&q='

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

def quote(s):
    return quote_plus(s.encode('utf-8'))

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
    return '&'.join('%s=%s' % (k, v) for k, v in args.items())

def test_changequery():
    with app.test_client() as c:
        rv = c.get('/?q=test&mediatype=movies&language=eng')
        assert request.args['q'] == 'test'
        assert request.args['mediatype'] == 'movies'
        assert request.args['language'] == 'eng'
        url = changequery(collection='nasa')
        assert url == 'q=test&mediatype=movies&language=eng&collection=nasa'
        url = changequery(language=None)
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

@app.route("/")
def do_search():
    q = request.args.get('q')
    results = None
    pages = None
    pages_in_set = 10
    half = None
    first_page_in_set = None
    last_page_in_set = None
    facet_args = [(f, request.args[f]) for f in facet_fields if f in request.args]
    facet_args_dict = dict(facet_args)
    page = 1
    if q:
        quote_q = quote(q)
        page = int(request.args.get('page', 1))
        start = results_per_page * (page-1)
        fq = ''.join('&fq=' + quote('{!tag=%s}%s:"%s"' % (f, f, request.args[f])) for f in facet_fields if f in request.args)
        date_range = request.args.get('date_range')
        date_facet = request.args.get('date_facet')
        if date_range:
            m = re_date_range.match(date_range)
            if m:
                start_year, end_year = m.groups()
                fq += '&fq=' + quote('date:[%s-01-01T00:00:00Z TO %s-01-01T00:00:00Z]' % (start_year, end_year))
        elif date_facet:
            fq += '&fq=' + quote('date:[%s-01-01T00:00:00Z TO %s-01-01T00:00:00Z+%dYEAR]' % (date_facet, date_facet, year_gap))

        url = solr_select_url + '&q=' + quote(q) + '&start=%d' % start + fq + ''.join('&facet.field='+('{!ex=' + f + '}' if f in facet_args_dict else '') + f for f in facet_fields)
        f = urlopen(url)
        reply = f.read()
        try:
            results = json.loads(reply)
        except:
            print reply
            raise

        num_found = results['response']['numFound']
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

    return render_template('search.html', q=q, page=page, results=results, \
            results_per_page=results_per_page, pages=pages, first_page_in_set=first_page_in_set, last_page_in_set=last_page_in_set, \
            quote=quote, comma=comma, int=int, facet_fields=facet_fields, lang_map=lang_map, facet_args=facet_args, \
            get_movie_thumb=get_movie_thumb, year_gap=year_gap, find_item=find_item, enumerate=enumerate, len=len, pick_best=pick_best, url=url, facet_args_dict=facet_args_dict,\
            get_img_thumb = get_img_thumb, changequery=changequery)
    
if __name__ == "__main__":
    app.run(host='0.0.0.0', port=8081, debug=True)
