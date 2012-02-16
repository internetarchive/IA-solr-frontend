from flask import Flask, render_template, request, redirect, Response, make_response, url_for
from urllib import urlopen, quote_plus
from pprint import pprint, pformat
import json, locale, sys, re, resource

from socket import socket, AF_INET, SOCK_DGRAM, SOL_UDP, SO_BROADCAST, timeout
from time import time

#megs = 200
#resource.setrlimit(resource.RLIMIT_AS, (megs * 1048576L, -1L))

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

facet_fields = ['noindex', 'mediatype', 'collection_facet', 'language_facet',
    'creator_facet', 'subject_facet', 'publisher_facet', 'licenseurl',
    'possible-copyright-status', 'rating_facet', 'sponsor_facet',
    'handwritten', 'tv_channel', 'tv_category', 'tv_program', 
    'tv_episode_name', 'tv_original_year', 'tv_starring',
    'court', ]
    # 'tuner', 'aspect_ratio', 'frames_per_second', 'audio_codec', 'video_codec', 'camera' ]

fl = ['identifier', 'creator', 'title', 'date', 'subject', 'collection',
    'scanner', 'mediatype', 'description', 'noindex', 'score', 'case-name',
    'rating', 'sponsor', 'imagecount', 'foldoutcount', 'downloads', 'date_str',
    'language', 'language_facet', 'item_filename']

list_fields = set(['collection', 'tv_starring', 'creator', 'language',
    'subject', 'contributor', 'uploader', 'boxid', 'isbn', 'oclc-id', 'lccn',
    'venue', 'coverage', 'transferer', 'updater'])

single_value_fields = set(['identifier', 'title', 'date', 'date_str',
    'scanner', 'mediatype', 'description', 'noindex',
    'foldoutcount', 'downloads', 'imagecount', 'publisher', 'sponsor',
    'mediatype', 'ppi', 'repub_state', 'item_size', 'handwritten',
    'operator', 'copyright-evidence-operator', 'scanningcenter',
    'possible-copyright-status', 'copyright-evidence', 'copyright-region',
    'licenseurl', 'source', 'tuner', 'previous_item', 'next_item', 'video_codec',
    'audio_codec', 'sample', 'frames_per_second', 'start_localtime',
    'start_time', 'stop_time', 'utf_offset', 'runtime', 'aspect_ratio',
    'scanfee', 'tv_channel', 'tv_category', 'tv_program', 'tv_episode_name',
    'tv_original_year', 'identifier-access', 'identifier-ark', 'venue',
    'coverage', 'transferer', 'addeddate', 'publicdate', 'sponsordate',
    'scandate', 'closed_captioning', 'nav_order', 'num_recent_reviews',
    'num_top_dl', 'spotlight_identifier', 'num_top_ba', 'shiptracking',
    'show_browse_title_link', 'show_browse_author_link',
    'rating', 'court', 'case-name', 'date-case-filed', 'date-case-terminated',
    'docket-num', 'pacer-case-num'])

solr_fields = set(fl + ['closed_captions'] + facet_fields)
re_field_pattern = re.compile('(' + '|'.join(solr_fields) + '):')

field_set = {
    'default': ['identifier', 'mediatype', 'title', 'date_str', 'collection',
        'downloads', 'item_size', 'item_file_format', 'uploader'],
    'all': ['identifier', 'mediatype', 'title', 'creator', 'date_str',
        'collection', 'downloads', 'item_size', 'item_file_format', 'uploader',
        'publisher', 'language', 'scanningcenter', 'subject', 'sponsor',
        'boxid', 'isbn', 'oclc-id', 'lccn', 'imagecount', 'foldoutcount',
        'repub_state', 'ppi', 'scandate', 'addeddate', 'publicdate',
        'sponsordate', 'contributor', 'scanner', 'operator',
        'case-name', 'court', 'date-case-filed', 'date-case-terminated',
        'docket-num', 'pacer-case-num', 'handwritten', 'source', 'tuner',
        'rating', 'tv_channel', 'tv_category', 'tv_program', 'tv_episode_name',
        'tv_original_year', 'tv_starring', 'audio_codec', 'video_codec',
        'frames_per_second', 'start_localtime', 'start_time', 'stop_time',
        'runtime', 'aspect_ratio', 'closed_captioning', 'nav_order',
        'num_recent_reviews', 'num_top_dl', 'spotlight_identifier',
        'num_top_ba', 'shiptracking', 'show_browse_title_link',
        'show_browse_author_link', 'taper'],
    'collection': ['identifier', 'mediatype', 'title', 'date_str',
        'collection', 'nav_order' ,'num_recent_reviews', 'num_top_dl',
        'spotlight_identifier', 'num_top_ba', 'shiptracking',
        'show_browse_title_link', 'show_browse_author_link'],
    'tv': ['identifier', 'mediatype', 'title', 'date_str', 'collection',
        'item_size', 'description', 'source', 'tuner', 'rating', 'tv_channel',
        'tv_category', 'tv_program', 'tv_episode_name', 'tv_original_year',
        'tv_starring', 'audio_codec', 'video_codec', 'frames_per_second',
        'start_localtime', 'start_time', 'stop_time', 'runtime',
        'aspect_ratio', 'closed_captioning'],
    'books': ['identifier', 'mediatype', 'title', 'creator', 'date_str',
        'collection', 'publisher', 'language', 'scanningcenter', 'subject',
        'sponsor', 'boxid', 'downloads', 'item_size', 'item_file_format',
        'isbn', 'oclc-id', 'lccn', 'imagecount', 'foldoutcount', 'repub_state',
        'ppi', 'scandate', 'addeddate', 'publicdate', 'sponsordate',
        'contributor', 'uploader', 'scanner', 'operator', 'handwritten'],
    'software': ['identifier', 'mediatype', 'title', 'date_str', 'collection',
        'publisher', 'subject', 'uploader', 'downloads', 'item_size',
        'item_file_format' ],
    'etree': ['identifier', 'mediatype', 'title', 'date_str', 'collection',
        'downloads', 'item_size', 'item_file_format', 'subject', 'source',
        'runtime', 'venue', 'coverage', 'uploader', 'transferer', 'updater'],
    'nasa': ['identifier', 'mediatype', 'title', 'date_str', 'collection',
        'downloads', 'item_size', 'item_file_format', 'subject', 'filename'],
    'usfederalcourts': ['identifier', 'mediatype', 'title', 'date_str',
        'collection', 'downloads', 'case-name', 'court', 'date-case-filed',
        'date-case-terminated', 'docket-num', 'pacer-case-num', 'item_size',
        'item_file_format'],
    'librivox': ['identifier', 'mediatype', 'title', 'creator', 'date_str',
        'collection', 'subject',
        'downloads', 'item_size', 'item_file_format',
        'addeddate', 'publicdate', 
        'uploader', 'source', 'taper'],

}
all_fields = set(field_set['all'] + ['date'])

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
    '&f.description.hl.fragsize=200' + \
    '&f.closed_captions.hl.fragsize=400' + \
    '&bq=' + quote('(*:* -collection:ourmedia -collection:opensource* collection:*)^10') + \
    '&q.op=AND'
#    '&spellcheck.collate=true' + \
#    '&spellcheck.maxCollationTries=5' + \
#    '&spellcheck.accuracy=0.5' + \
#    '&facet.range=imagecount&f.imagecount.facet.range.start=0&f.imagecount.facet.range.end=1000000&f.imagecount.facet.range.gap=100' + \
#    '&facet.range=downloads&f.downloads.facet.range.start=0&f.downloads.facet.range.end=1000000&f.downloads.facet.range.gap=100,10000' + \

facet_enum_fields = [ 'noindex', 'language_facet', 'mediatype', 'tv_category', 'tv_channel', 'language_facet', 'handwritten', 'scanningcenter' ]

# '&f.' + f + '.facet.method=enum' 

facet_params = '&facet=true&facet.limit=20&facet.mincount=1' + \
    '&f.year_from_date.facet.sort=index' + \
    '&f.year_from_date.facet.limit=-1' + \
    '&f.noindex.facet.sort=index' + \
    '&f.noindex.facet.mincount=0' + \
    '&f.tv_original_year.facet.sort=index' + \
    '&f.tv_original_year.facet.limit=-1' + \
    '&facet.range=date&f.date.facet.range.start=0000-01-01T00:00:00Z&f.date.facet.range.end=2015-01-01T00:00:00Z&f.date.facet.range.gap=%2B' + str(year_gap) + 'YEAR'

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

def add_to_field(field, value):
    args = dict((k, v) for k, v in request.args.items() if v)
    if field in args:
        args[field] += ' ' + value
    else:
        args[field] = ' ' + value
    return '&'.join('%s=%s' % (k, quote(v)) for k, v in args.items())

def changequery(new_args):
    args = dict((k, v) for k, v in request.args.items() if v and k not in new_args)
    args.update(dict((k, v) for k, v in new_args.items() if v is not None))
    return '&'.join('%s=%s' % (k, quote(v)) for k, v in args.items())

def zap_field(fields, cur):
    return changequery({'field_set': None, 'fields': ','.join(f for f in fields if f != cur)})

def selected_fields():
    if request.args.get('field_set'):
        fields = field_set[request.args['field_set']]
    elif request.args.get('fields',''):
        fields = request.args.get('fields','').split(',')
    else:
        fields = field_set['default']
    extra = request.args.get('extra')
    if not extra:
        return fields
    extra = [i for i in extra.split(',') if i]

    for f in 'mediatype', 'identifier':
        if f in fields:
            index = fields.index(f)
            return fields[:index] + extra + fields[index:]
    return extra + fields

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

def build_pager(num_found, page, pages_in_set = 10, rows=results_per_page):
    pages = (num_found / rows) + 1
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

def get_collections(results):
    collections = set()
    try:
        collections = set(key for key, num in results['facet_counts']['facet_fields']['collection_facet'])
    except KeyError:
        pass
    for doc in results['response']['docs']:
        collections.update(doc.get('collection', []))

    if not collections:
        return {}
    collections = list(collections)

    docs = []
    while collections:
        cur = collections[:50]
        del collections[:50]
        url = 'http://' + addr + '/solr/select?wt=json' + \
            '&json.nl=arrarr' + \
            '&defType=edismax' + \
            '&qf=identifier' + \
            '&q=' + quote(' OR '.join(cur)) + \
            '&fl=identifier,title,hidden,access-restricted' + \
            '&rows=%d' % len(cur)
        reply = urlopen(url).read()
        try:
            data = json.loads(reply)
        except ValueError:
            raise SolrError(reply)
        docs += data['response']['docs']
    return docs

def get_collection_titles(results):
    return dict((c['identifier'], c['title']) for c in get_collections(results))

@app.route('/mlt/<identifier>')
def mlt_page(identifier):
    url = 'http://' + addr + '/solr/mlt?wt=json' + \
        '&json.nl=arrarr' + \
        '&q=identifier:' + quote(identifier) + \
        '&mlt.fl=creator,title,subject,collection,mediatype,description,' + \
            'case-name,sponsor,closed_captions' + \
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
    return render_template('mlt.html', identifier=identifier, mlt=data,
            get_movie_thumb=get_movie_thumb, pick_best=pick_best,
            collection_titles=collection_titles, len=len)

re_all_field = re.compile(r'^([A-Za-z0-9_-]+):\*')
def search(q, url_params, spellcheck=True, facets=False, rows=results_per_page, grid=False):
    url = solr_select_url + '&q=' + quote(q) + url_params
    if facets:
        url += facet_params
        url += ''.join('&f.' + f + '.facet.method=enum' for f in facet_enum_fields)
    url += '&rows=' + str(rows)
    if grid:
        cur_fields = list(selected_fields())
        for f in 'collection', 'noindex', 'scanner', 'item_filename':
            if f not in cur_fields:
                cur_fields.append(f)
        url += '&fl=' + ','.join(cur_fields)
    else:
        url += '&fl=' + ','.join(fl)

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


re_to_esc = re.compile(r'[\[\]:()]')
def esc(s):
    if not isinstance(s, basestring):
        return s
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

re_convert_to_range = re.compile('^\s*([<>])\s*(-?\d+)\s*$')

def fmt_filesize(value, binary=False):
    """Format the value like a 'human-readable' file size (i.e. 13 kB,
4.1 MB, 102 Bytes, etc). Per default decimal prefixes are used (Mega,
Giga, etc.), if the second parameter is set to `True` the binary
prefixes are used (Mebi, Gibi).
"""
    bytes = float(value)
    base = binary and 1024 or 1000
    prefixes = [
        (binary and 'KiB' or 'kB'),
        (binary and 'MiB' or 'MB'),
        (binary and 'GiB' or 'GB'),
        (binary and 'TiB' or 'TB'),
        (binary and 'PiB' or 'PB'),
        (binary and 'EiB' or 'EB'),
        (binary and 'ZiB' or 'ZB'),
        (binary and 'YiB' or 'YB')
    ]
    if bytes == 1:
        return '1 Byte'
    elif bytes < base:
        return '%d Bytes' % bytes
    else:
        for i, prefix in enumerate(prefixes):
            unit = base ** (i + 2)
            if bytes < unit:
                return '%.2f %s' % ((base * bytes / unit), prefix)
        return '%.2f %s' % ((base * bytes / unit), prefix)

def grid_field(f):
    v = request.args[f]
    if f in ('date_str', 'date'):
        return ('date_str', esc(v))
    m = re_convert_to_range.match(v)
    if not m:
        return (f, v)
    if m.group(1) == '<':
        return (f, '[* TO ' + m.group(2) + ']')
    else:
        assert m.group(1) == '>'
        return (f, '[' + m.group(2) + ' TO *]')

def add_hidden_tag(docs, collections):
    for doc in docs:
        if 'collection' not in doc:
            continue
        k = 'access-restricted'
        if any(k in collections.get(c, {}) and collections[c][k][0] == 'true' for c in doc['collection']):
            doc[k] = 'true'

@app.route("/collection/<collection>")
def collection_page(collection):

    t0 = time()
    url = 'http://' + addr + '/solr/select?wt=json' + \
        '&json.nl=arrarr' + \
        '&defType=edismax' + \
        '&qf=identifier' + \
        '&q=' + quote(collection) + \
        '&fl=identifier,title,hidden,access-restricted' + \
        '&rows=1'
    ret = urlopen(url).read()
    try:
        data = json.loads(ret)
    except ValueError:
        return ret
    title = data['response']['docs'][0]['title']

    url = 'http://' + addr + '/solr/select?wt=json' + \
        '&json.nl=arrarr' + \
        '&q=collection:' + quote(collection) + \
        '&fl=identifier' + \
        '&indent=on' + \
        '&stats=on' + \
        '&stats.field=item_size' + \
        '&stats.field=downloads' + \
        '&rows=0'
    ret = urlopen(url).read()
    try:
        data = json.loads(ret)
    except ValueError:
        return ret
    t_solr = time() - t0

    #return Response(pformat(data['stats']['stats_fields']), mimetype='text/plain')
    return render_template('collection.html', collection=collection,
            title=title, comma=comma, fmt_filesize=fmt_filesize, results=data,
            t_solr=t_solr)

@app.route("/fields")
def select_fields_page():
    return render_template('select_fields.html',
        default_fields=field_set['default'],
        all_fields=field_set['all']
    )

@app.route("/facet/<field>")
def facet_page(field):
    field = field.lower()
    if field in facet_fields:
        facet_field = field
    elif field + '_facet' in facet_fields:
        facet_field = field + '_facet'
    else:
        return render_template('facet.html', field=field, changequery=changequery)
    rows = 0
    search_fields = [grid_field(f) for f in all_fields if request.args.get(f)]
    fq = ''.join('&fq=' + quote('%s:(%s)' % i) for i in search_fields)
    q = '*:*'
    url_params = fq
    url_params += '&facet=true&facet.mincount=1&facet.limit=-1&facet.sort=count' 
    url_params += '&facet.field=' + facet_field
    url_params += ''.join('&f.' + f + '.facet.method=enum' for f in facet_enum_fields)

    search_results = search(q, url_params, spellcheck=False, rows=rows)
    results = search_results['results']
    return render_template('facet.html', field=field,
        counts=results['facet_counts']['facet_fields'][facet_field],
        t_solr=search_results['t_solr'], changequery=changequery,
        add_to_field=add_to_field, solr_esc=esc, search_fields=search_fields
    )

@app.route("/grid")
def grid_page():
    new_query_string = '&'.join(k + '=' + v for k, v in (i.split('=', 1) for i in request.query_string.split('&') if '=' in i) if v != '')
    if request.query_string != new_query_string:
        return redirect(url_for('grid_page') + '?' + new_query_string)

    page = int(request.args.get('page', 1))

    extra = request.args.get('extra', '').split(',')
    fields = selected_fields()

    grid_facet_fields = []
    for f in fields:
        if f == 'scanningcenter' or f in facet_fields:
            grid_facet_fields.append(f)
        elif f + '_facet' in facet_fields:
            grid_facet_fields.append(f + '_facet')

    rows = int(request.args.get('rows', 50))
    start = rows * (page-1)
    fq = ''.join('&fq=' + quote('%s:(%s)' % grid_field(f)) for f in all_fields if request.args.get(f))
    if not fq:
        return render_template('grid.html', changequery=changequery,
            field_set=field_set, zap_field=zap_field, page=page, fields=fields,
            results=[], results_per_page=50, 
            quote=quote, extra=extra, comma=comma, len=len, 
            list_fields=list_fields, 
            single_value_fields=single_value_fields, int=int,
            solr_esc=esc, isinstance=isinstance, basestring=basestring, rows=rows,
            fmt_filesize=fmt_filesize)

    url_params = '&start=%d' % start + fq
    url_params += '&facet=true&facet.limit=20&facet.mincount=1' 
    url_params += ''.join('&facet.field=' + f for f in grid_facet_fields)
    url_params += ''.join('&f.' + f + '.facet.method=enum' for f in facet_enum_fields)
    q = request.args.get('q', '*:*')
    try:
        search_results = search(q, url_params, spellcheck=False, rows=rows, grid=True)
    except SolrError as solr_error:
        return solr_error.value

    results = search_results['results']
    collections = dict((c['identifier'], c) for c in get_collections(results))

    add_hidden_tag(results['response']['docs'], collections)
    t_solr = search_results['t_solr']
    url = search_results['url']
    pager = build_pager(results['response']['numFound'], page, rows=rows)

    for doc in results['response']['docs']:
        if doc.get('mediatype') == 'texts' and doc.get('scanner'):
            doc['thumb_path'] = '/page/{{doc.identifier}}_cover_h80.jpg'
        else:
            first_img = None
            first_logo = None
            for filename in doc['item_filename']:
                if filename.endswith('thumb.jpg'):
                    doc['thumb_path'] = filename
                    break
                lc_filename = filename.lower()
                if not first_img and any(lc_filename.endswith('.' + ext) for ext in ('jpg', 'jpeg', 'png')):
                    first_img = filename
                if not first_logo and any(lc_filename.endswith('logo.' + ext) for ext in ('jpg', 'jpeg', 'png')):
                    first_logo = filename
            if not doc.get('thumb_path'):
                if first_logo:
                    doc['thumb_path'] = first_logo
                elif first_img:
                    doc['thumb_path'] = first_img

    return render_template('grid.html', changequery=changequery,
        field_set=field_set, zap_field=zap_field, page=page, fields=fields,
        results=results, results_per_page=50, pager=pager, t_solr=t_solr,
        quote=quote, extra=extra, comma=comma, len=len, list_fields=list_fields, 
        single_value_fields=single_value_fields, int=int,
        solr_esc=esc, isinstance=isinstance, basestring=basestring, rows=rows,
        fmt_filesize=fmt_filesize)

@app.route("/")
def search_page():
    valid_views = set(['search', 'grid', 'thumb_compare'])
    view = request.args.get('view', 'search')
    if view == 'grid':
        new_query_string = '&'.join(k + '=' + v for k, v in (i.split('=', 1) for i in request.query_string.split('&') if '=' in i) if v != '' and k != 'view')
        return redirect(url_for('grid_page') + '?' + new_query_string)

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
        search_results = search(q, url_params, spellcheck=True, facets=True)
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
            search_results = search(new_q, url_params, spellcheck=False, facets=True)
        except SolrError as solr_error:
            return solr_error.value
        alt_results = True

    t0_solr = time()
    collections = dict((c['identifier'], c) for c in get_collections(results))
    add_hidden_tag(results['response']['docs'], collections)
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
        get_img_thumb = get_img_thumb, changequery=changequery,
        zap_field=zap_field, token_hl=token_hl, t_solr=t_solr,
        collections=collections, did_you_mean=did_you_mean,
        alt_results=alt_results, fmt_licenseurl=fmt_licenseurl,
        add_thumbs_to_docs=add_thumbs_to_docs,
        strip_long_repeating_phrase=strip_long_repeating_phrase,
        list_fields=list_fields, field_set=field_set,
        date_facet=(int(date_facet) if date_facet is not None else None))
    
if __name__ == "__main__":
    app.run(host='0.0.0.0', port=8081, debug=True)
