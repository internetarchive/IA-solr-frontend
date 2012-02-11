from flask import Flask, render_template, request, redirect, Response
from urllib import urlopen, quote_plus
import json, locale, sys, re

from socket import socket, AF_INET, SOCK_DGRAM, SOL_UDP, SO_BROADCAST, timeout
from time import time

# Copyright(c)2012 Internet Archive. Software license GPL version 2.
# Written by Edward Betts <edward@archive.org>

app = Flask(__name__)

re_loc = re.compile('^(ia\d+\.us\.archive\.org):(/\d+/items/(.*))$')

class FindItemError(Exception):
    pass

class SolrError(Exception):
    def __init__(self, value):
        self.value = value

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

facet_fields = ['noindex', 'mediatype', 'collection', 'language_facet',
    'creator_facet', 'subject_facet', 'publisher_facet', 'licenseurl',
    'possible-copyright-status', 'rating_facet', 'sponsor_facet',
    'handwritten', 'source', 'tv_channel', 'tv_category', 'tv_program', 
    'tv_episode_name', 'tv_original_year', 'tv_starring', ]
    # 'tuner', 'aspect_ratio', 'frames_per_second', 'audio_codec', 'video_codec', 'camera' ]

fl = ['identifier', 'creator', 'title', 'date', 'subject', 'collection',
    'scanner', 'mediatype', 'description', 'noindex', 'score', 'case-name',
    'rating', 'sponsor', 'imagecount', 'foldoutcount', 'downloads', 'date_str',
    'language', 'language_facet', 'item_filename']

list_fields = set(['collection', 'tv_starring', 'creator', 'language', 'subject', 'contributor', 'uploader'])

solr_fields = set(fl + ['closed_captions'] + facet_fields)
re_field_pattern = re.compile('(' + '|'.join(solr_fields) + '):')

field_set = {
    'default': ['identifier', 'mediatype', 'title', 'date_str', 'collection', 'downloads'],
    'tv': ['identifier', 'title', 'date_str', 'collection', 'source', 'tuner',
        'rating', 'tv_channel', 'tv_category', 'tv_program', 'tv_episode_name',
        'tv_original_year', 'tv_starring', 'audio_codec', 'video_codec',
        'frames_per_second', 'start_localtime', 'start_time', 'stop_time',
        'runtime', 'aspect_ratio', 'closed_captioning'],
    'books': ['identifier', 'title', 'creator', 'date_str', 'collection',
        'publisher', 'language', 'scanningcenter', 'subject', 'sponsor',
        'imagecount', 'foldoutcount', 'repub_state', 'ppi', 'scandate',
        'addeddate', 'publicdate', 'sponsordate', 'contributor', 'uploader',
        'scanner', 'operator', 'downloads', 'handwritten'],
    'software': ['identifier', 'title', 'date_str', 'collection',
        'publisher', 'subject', 'uploader', 'downloads', ],
    'all': ['identifier', 'mediatype', 'title', 'creator', 'date_str', 'collection',
        'publisher', 'language', 'scanningcenter', 'subject', 'sponsor',
        'imagecount', 'foldoutcount', 'repub_state', 'ppi', 'scandate',
        'addeddate', 'publicdate', 'sponsordate', 'contributor', 'uploader',
        'scanner', 'operator', 'downloads', 'handwritten', 'source', 'tuner',
        'rating', 'tv_channel', 'tv_category', 'tv_program', 'tv_episode_name',
        'tv_original_year', 'tv_starring', 'audio_codec', 'video_codec',
        'frames_per_second', 'start_localtime', 'start_time', 'stop_time',
        'runtime', 'aspect_ratio', 'closed_captioning'],
}

year_gap = 10

results_per_page = 30 

def quote(s):
    return quote_plus(s.encode('utf-8')) if not isinstance(s, int) else s

solr_hl = '&hl=true' + \
    '&hl.snippets=1' + \
    '&hl.fragsize=0' + \
    '&f.description.hl.alternateField=description' + \
    '&f.closed_captions.hl.alternateField=closed_captions' + \
    '&f.closed_captions.hl.maxAlternateFieldLength=200' + \
    '&hl.fl=title,creator,subject,collection,description,case-name,closed_captions&hl.simple.pre=' + quote('{{{') + '&hl.simple.post=' + quote('}}}')

addr = 'ol-search-inside:8984'
addr = 'localhost:6081'
solr_select_url = 'http://' + addr + '/solr/select?wt=json' + \
    '&json.nl=arrarr' + \
    '&defType=edismax' + \
    '&qf=text+closed_captions' + \
    '&facet=true&facet.limit=20&facet.mincount=1' + \
    '&f.language_facet.facet.method=enum' + \
    '&f.mediatype.facet.method=enum' + \
    '&f.tv_category.facet.method=enum' + \
    '&f.tv_channel.facet.method=enum' + \
    '&f.language_facet.facet.method=enum' + \
    '&f.year_from_date.facet.sort=index' + \
    '&f.noindex.facet.sort=index' + \
    '&f.noindex.facet.mincount=0' + \
    '&f.year_from_date.facet.limit=-1' + \
    '&f.tv_original_year.facet.sort=index' + \
    '&f.tv_original_year.facet.limit=-1' + \
    '&facet.range=date&f.date.facet.range.start=0000-01-01T00:00:00Z&f.date.facet.range.end=2015-01-01T00:00:00Z&f.date.facet.range.gap=%2B' + str(year_gap) + 'YEAR' + \
    '&f.description.hl.fragsize=200' + \
    '&f.closed_captions.hl.fragsize=400' + \
    '&bq=(*:* -collection:ourmedia -collection:opensource* collection:*)^10' + \
    '&q.op=AND'
#    '&spellcheck.collate=true' + \
#    '&spellcheck.maxCollationTries=5' + \
#    '&spellcheck.accuracy=0.5' + \
#    '&facet.range=imagecount&f.imagecount.facet.range.start=0&f.imagecount.facet.range.end=1000000&f.imagecount.facet.range.gap=100' + \
#    '&facet.range=downloads&f.downloads.facet.range.start=0&f.downloads.facet.range.end=1000000&f.downloads.facet.range.gap=100,10000' + \

lang_map = {
    'eng': 'English', 'fre': 'French', 'ger': 'German', 'spa': 'Spanish',
    'ita': 'Italian', 'ota': 'Ottoman Turkish', 'kor': 'Korean',
    'lat': 'Latin', 'rus': 'Russian', 'dut': 'Dutch', 'ara': 'Arabic',
    'swe': 'Swedish', 'por': 'Portuguese', 'dan': 'Danish', 'hun': 'Hungarian',
    'cze': 'Czech', 'tel': 'Telugu', 'pol': 'Polish', 'urd': 'Urdu',
    'nor': 'Norwegian', 'rum': 'Romanian', 'ice': 'Icelandic', 
    'hrv': 'Croatian', 'arm': 'Armenian', 'srp': 'Serbian', 'swa': 'Swahili',
    'ind': 'Indonesian', 'may': 'Malay', 'slv': 'Slovenian', 'tur': 'Turkish',
    'fin': 'Finnish', 'wel': 'Welsh', 'bul': 'Bulgarian', 'afr': 'Afrikaans',
    'slo': 'Slovak', 'cat': 'Catalan', 'san': 'Sanskrit', 'rum': 'Romanian',
    'hin': 'Hindi', 'chi': 'Chinese', 'vie': 'Vietnamese', 'glg': 'Galician',
    'tam': 'Tamil', 'jpn': 'Japanese', 'tgl': 'Tagalog', 'baq': 'Basque',
    'heb': 'Hebrew', 'gle': 'Irish', 'kan': 'Kannada', 'ger': 'Deutsch',
    'bos': 'Bosnian', 'ukr': 'Ukrainian',
    'mlt': 'Maltese', 'est': 'Estonian', 'aze': 'Azerbaijani', 'ban': 'Balinese',
    'lit': 'Lithuanian', 'por': 'Portuguese', 'alb': 'Albanian', 'tha': 'Thai',
    'gre': 'Greek', 'grc': 'Ancient Greek', 'gae': 'Scottish Gaelic',
    'mul': 'Multiple', 'und': 'Undefined', 'oji': 'Ojibwa', 'yid': 'Yiddish',
    'zxx': 'No linguistic content',
    'english-handwritten': 'English (handwritten)',
}

rev_lang_map = dict((v.lower(), k) for k, v in lang_map.iteritems())

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

def fmt_licenseurl(url):
    cc_start = 'http://creativecommons.org/licenses/'
    if url.startswith('http://creativecommons.org/publicdomain/zero/1.0'):
        return 'CC0 1.0'
    if url.startswith('http://creativecommons.org/') and 'publicdomain' in url.lower():
        return 'Public domain'
    if url.startswith(cc_start):
        return 'CC ' + url[len(cc_start):].upper().replace('/', ' ').strip()
    return url

def test_fmt_licenseurl():
    url = 'http://creativecommons.org/licenses/by-nc-nd/3.0/us/'
    label = 'Creative Commons BY-NC-ND 3.0 US'
    assert fmt_licenseurl(url) == label
    assert fmt_licenseurl('x') == 'x'


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

def parse_suggestions(q, suggestions):
    reply = []
    xpos = 0
    for word, s in suggestions:
        if s['startOffset'] < xpos:
            continue
        if s['startOffset'] > xpos:
            reply.append(('orig', q[xpos:s['startOffset']]))
        reply.append(('fix', s['suggestion'][0]))
        xpos = s['endOffset']
    if xpos != len(q):
        reply.append(('orig', q[xpos:]))
    return reply

def test_parse_suggestions():
    q = 'foodd'
    suggestions = [[u'foodd',
                   {u'endOffset': 5,
                    u'numFound': 1,
                    u'startOffset': 0,
                    u'suggestion': [u'food']}]]
    expect = [('fix', 'food')]
    assert parse_suggestions(q, suggestions) == expect

    q = 'unnitted stattes'
    suggestions = [[u'unnitted',
                   {u'endOffset': 8,
                    u'numFound': 1,
                    u'startOffset': 0,
                    u'suggestion': [u'united']}]]

    expect = [('fix', 'united'), ('orig', ' stattes')]
    assert parse_suggestions(q, suggestions) == expect

    q = 'unnitted stetes'
    suggestions = [[u'unnitted',
                   {u'endOffset': 8,
                    u'numFound': 1,
                    u'startOffset': 0,
                    u'suggestion': [u'united']}],
                  [u'stetes',
                   {u'endOffset': 15,
                    u'numFound': 1,
                    u'startOffset': 9,
                    u'suggestion': [u'states']}]]
    expect = [('fix', 'united'), ('orig', ' '), ('fix', 'states')]
    assert parse_suggestions(q, suggestions) == expect

    q = "united stats"
    suggestions = [["united stats",{
                    "numFound":1,
                    "startOffset":0,
                    "endOffset":12,
                    "suggestion":["united states"]}],
                  ["stats",{
                    "numFound":1,
                    "startOffset":7,
                    "endOffset":12,
                    "suggestion":["states"]}]]
    expect = [('fix', 'united states')]
    assert parse_suggestions(q, suggestions) == expect

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

def zap_field(fields, cur):
    return changequery({'field_set': None, 'fields': ','.join(f for f in fields if f != cur)})

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
    collections = set()
    if 'facet_counts' in results:
        collections = set(key for key, num in results['facet_counts']['facet_fields']['collection'])
    for doc in results['response']['docs']:
        collections.update(doc.get('collection', []))

    if not collections:
        return {}

    url = 'http://' + addr + '/solr/select?wt=json' + \
        '&json.nl=arrarr' + \
        '&defType=edismax' + \
        '&qf=identifier' + \
        '&q=' + quote(' OR '.join(collections)) + \
        '&fl=identifier,title' + \
        '&rows=%d' % len(collections)
    reply = urlopen(url).read()
    try:
        data = json.loads(reply)
    except ValueError:
        raise SolrError(reply)
    return dict((c['identifier'], c['title']) for c in data['response']['docs'])

@app.route('/mlt/<identifier>')
def view_mlt(identifier):
    url = 'http://' + addr + '/solr/mlt?wt=json' + \
        '&json.nl=arrarr' + \
        '&q=identifier:' + quote(identifier) + \
        '&mlt.fl=creator,title,subject,collection,mediatype,description,case-name,sponsor,closed_captions' + \
        '&mlt.mintf=1' + \
        '&fl=score,*' + \
        '&indent=on' + \
        '&rows=200'
    ret = urlopen(url).read()
    try:
        data = json.loads(ret)
    except ValueError:
        return ret
    collection_titles = get_collection_titles(data)
    return render_template('mlt.html', identifier=identifier, mlt=data, get_movie_thumb=get_movie_thumb, pick_best=pick_best, collection_titles=collection_titles, len=len)

re_all_field = re.compile(r'^([A-Za-z0-9_-]+):\*')
def search(q, url_params, spellcheck=True):
    url = solr_select_url + '&q=' + quote(q) + url_params
    url += '&rows=' + ('100' if request.args.get('view') == 'grid' else str(results_per_page))
    if request.args.get('field_set'):
        url += '&fl=' + ','.join(fl + field_set[request.args['field_set']])
    else:
        url += '&fl=' + ','.join(fl) + request.args.get('fields','')

    if spellcheck:
        #spellcheck_q = quote(re_field_pattern.sub('', q))
        #print spellcheck_q
        url += '&spellcheck=true&spellcheck.count=1'
        #url += '&spellcheck=true&spellcheck.count=1&spellcheck.onlyMorePopular=true&spellcheck.q=' + quote(q)
    if q != '*:*' and not re_all_field.match(q):
        url += solr_hl
    if request.args.get('debug'):
        url += '&debugQuery=true'
    sort = request.args.get('sort')
    if sort:
        url += '&sort=' + quote(sort)
    t0_solr = time()
    f = urlopen(url)
    reply = f.read()
    t_solr = time() - t0_solr
    try:
        results = json.loads(reply)
    except ValueError:
        raise SolrError(reply)
    return {'url': url, 'results': results, 't_solr': t_solr}


re_to_esc = re.compile(r'[\[\]:]')
def esc(s):
    return re_to_esc.sub(lambda m:'\\' + m.group(), s)

@app.route("/thumbs/<identifier>")
def html_thumbs(identifier):
    thumb = get_movie_thumb(identifier)
    return ''.join('<img src="' + thumb['url'] + img + '">' for img in thumb['imgs'])

def add_thumbs_to_docs(docs):
    max_len = 0
    for doc in docs:
        if doc['mediatype'] != 'movies':
            continue
        thumbs = get_movie_thumb(doc['identifier'])
        if not thumbs:
            continue
        doc['thumbs'] = thumbs
        if len(doc['thumbs']['imgs']) > max_len:
            max_len = len(doc['thumbs']['imgs'])

    return max_len

@app.route("/collection_autocomplete")
def collection_autocomplete():
    if 'term' not in request.args:
        return '[]'
    term = request.args['term'].lower()
    found = []
    for line in open('/home/edward/src/ia_solr_frontend/collections'):
        if line.lower().startswith(term):
            found.append(line[:-1])
        elif found:
            break
    return Response(json.dumps(found), mimetype='application/json')

re_long_repeating_phrase = re.compile('(.{6}){4,}')
def strip_long_repeating_phrase(s):
    return re_long_repeating_phrase.sub(lambda m:m.group(1), s)

def fix_language(k, v):
    if k != 'language_facet' or not rev_lang_map.get(v.lower()):
        return v
    return rev_lang_map[v.lower()]

@app.route("/")
def do_search():
    valid_views = set(['search', 'grid', 'thumb_compare'])
    view = request.args.get('view', 'search')
    if view not in valid_views:
        view = 'search'

    q = request.args.get('q')
    facet_args = [(f, request.args[f]) for f in facet_fields if request.args.get(f)]

    if len(request.args) > 1 and view=='grid' and not q:
        if request.args.get('identifier'):
            q = 'identifier:' + request.args['identifier']
        else:
            q = '*:*'
    if not q:
        return render_template(view + '.html', lang_map=lang_map, changequery=changequery, zap_field=zap_field, field_set=field_set)
    q = q.strip()

    new_query_string = '&'.join(k + '=' + fix_language(k, v) for k, v in (i.split('=', 1) for i in request.query_string.split('&') if '=' in i) if v != '')
    if request.query_string != new_query_string:
        return redirect('?' + new_query_string)

    facet_args_dict = dict(facet_args)
    page = int(request.args.get('page', 1))
    start = results_per_page * (page-1)
    #fq = ''.join('&fq=' + quote('{!tag=%s}{!term f=%s}%s' % (f, f, request.args[f])) for f in facet_fields if f in request.args)
    fq = ''.join('&fq=' + quote('{!tag=%s}%s:"%s"' % (f, f, esc(request.args[f]))) for f in facet_fields if request.args.get(f))
    date_range = request.args.get('date_range')
    date_facet = request.args.get('date_facet')
    date_from = request.args.get('date_from')
    date_to = request.args.get('date_to')
    if date_range:
        m = re_date_range.match(date_range)
        if m:
            start_year, end_year = m.groups()
            fq += '&fq=' + quote('date:[%s-01-01T00:00:00Z TO %s-01-01T00:00:00Z]' % (start_year, end_year))
    elif date_facet:
        fq += '&fq=' + quote('date:([%s-01-01T00:00:00Z TO %s-01-01T00:00:00Z+%dYEAR] NOT "%s-01-01T00:00:00Z+%dYEAR")' % (date_facet, date_facet, year_gap, date_facet, year_gap))
    elif date_from and date_to:
        fq += '&fq=' + quote('date:[%sT00:00:00Z TO %sT00:00:00Z]' % (date_from, date_to))

    url_facet_fields = ''.join('&facet.field='+('{!ex=' + f + '}' if f in facet_args_dict else '') + f for f in facet_fields)

    url_params = '&start=%d' % start + fq + url_facet_fields
    try:
        search_results = search(q, url_params, spellcheck=True)
    except SolrError as solr_error:
        return solr_error.value
    results = search_results['results']
    t_solr = search_results['t_solr']

    alt_results = False
    did_you_mean = []
    if results.get('spellcheck', {}).get('suggestions'):
        did_you_mean = parse_suggestions(q, results['spellcheck']['suggestions'])
    if results['response']['numFound'] == 0 and did_you_mean and 'nfpr' not in request.args:
        new_q = ''.join(i[1] for i in did_you_mean)
        try:
            search_results = search(new_q, url_params, spellcheck=False)
        except SolrError as solr_error:
            return solr_error.value
        alt_results = True

    t0_solr = time()
    collection_titles = get_collection_titles(results)
    t_solr += time() - t0_solr

    pager = build_pager(results['response']['numFound'], page)

    url = search_results['url']
    results = search_results['results']
    t_solr += search_results['t_solr']


    for f in 'tv_original_year', 'year_from_date':
        try:
            results['facet_counts']['facet_fields'][f].reverse()
        except KeyError:
            pass

    return render_template(view + '.html', q=q, page=page, 
        results=results, results_per_page=results_per_page, pager=pager,
        quote=quote, comma=comma, int=int, facet_fields=facet_fields, 
        lang_map=lang_map, facet_args=facet_args,
        get_movie_thumb=get_movie_thumb, year_gap=year_gap,
        find_item=find_item, enumerate=enumerate, len=len,
        pick_best=pick_best, url=url, facet_args_dict=facet_args_dict,
        get_img_thumb = get_img_thumb, changequery=changequery, zap_field=zap_field,
        token_hl=token_hl, t_solr=t_solr, collection_titles=collection_titles,
        did_you_mean=did_you_mean, alt_results=alt_results,
        fmt_licenseurl=fmt_licenseurl, add_thumbs_to_docs=add_thumbs_to_docs,
        strip_long_repeating_phrase=strip_long_repeating_phrase,
        list_fields=list_fields, field_set=field_set,
        date_facet=(int(date_facet) if date_facet is not None else None))
    
if __name__ == "__main__":
    app.run(host='0.0.0.0', port=8081, debug=True)
