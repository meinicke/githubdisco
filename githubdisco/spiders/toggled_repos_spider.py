import logging

import re
import string
import os
import scrapy
import json
import re
import base64
from string import Template
from libraries import LIBRARIES
from scrapy.shell import inspect_response

# Find toggled repositories via GitHub v3 API
#
# Usage:
# $ AUTH_TOKEN=... scrapy crawl toggled_repos -o ../results/normalized/results-github-scraper-`date -u "+%Y%m%d%H%M%S"`.csv

# scrapy crawl toggled_repos -o ../results/results-github-patreon.csv

def java_placeholders(_placeholders):
    placeholders = dict(_placeholders)
    group_id, artifact_id = placeholders['artifact_name'].split(',', 1)[0].split(':', 1)
    placeholders['group_id'] = group_id
    placeholders['artifact_id'] = artifact_id
    return placeholders


class ToggledReposSpider(scrapy.Spider):
    name = "toggled_repos"

    csv_fieldnames = ['repo_name', 'path', 'language', 'size_bytes', 'library', 'library_language', 'last_commit_ts', 'forked_from']

    token_ID = 0

    tokens = [os.environ['Github_1'], os.environ['Github_2'], os.environ['Github_3'], os.environ['Github_4'],
              os.environ['Github_5'], os.environ['Github_6'], os.environ['Github_7'], os.environ['Github_8'],
              os.environ['Github_9'], os.environ['Github_10']]

    def get_headers(self, new_token):
        if new_token:
            self.token_ID += 1
        return {
            'Authorization': 'Bearer ' + self.get_GH_TOKEN(),
        }

    def get_GH_TOKEN(self):
        return self.tokens[self.token_ID % len(self.tokens)]

    size_from = 0
    # Only files smaller than 384 KB are searchable.
    size_to = 1000000

    libraries = LIBRARIES

    extensions_by_lang = {
        'javascript':   ['json', 'js', 'jsx', 'ts', 'tsx'], # also: TypeScript
        'c#':           ['json', 'config', 'csproj', 'vbproj'], # also Visual Basic
        'java':         ['xml', 'java', 'gradle', 'gradle.kts'], # also: Kotlin
        'objective-c':  ['m', 'h', 'swift'], # also: Swift
        'php':          ['json', 'php'],
        'python':       ['py'],
        'ruby':         ['rb'],
        'scala':        ['sbt', 'scala', 'sc'],
        'go':           ['go'],
        'markdown':     ['md'],
    }

    filenames_by_lang = {
        'objective-c':  ['Podfile', 'Cartfile'], # also: Swift
        'ruby':         ['Gemfile'],
        'markdown':     ['readme.md']
    }

    # Regular expression templates have a structure to make them more flexible:
    #
    # [
    #   r'content_template_regexp',
    #   flags,
    #   r'path, # Exclusive regexp to match content in the path
    #   augment_placeholders_fn, # function augmenting the placeholders available to content regexp
    # ]
    #
    # ${import_or_usage} placeholders fallback to ${artifact_name} if not present
    #
    # All placeholders are escaped by re.escape
    regexp_templates_by_lang = {
        'javascript': [
            ['(?:devDependencies|dependencies)":[\S\W]*"${artifact_name}"', None, r'json$'],
            ['(?:require.+|import.+|from.+)(?:"|\')${artifact_name}(?:"|\')', None, r'(?:js|jsx|ts|tsx)$'],
        ],
        'c#': [
            ['(?:<package\s*id=|<PackageReference\s*Include=|<Reference\s*Include=)"${artifact_name}', re.IGNORECASE, r'(?:config|csproj|vbproj)$'],
            ['dependencies":[\S\W]*"${artifact_name}"', None, r'json$'],
        ],
        'java': [
            ['compile.+${artifact_name}', None, r'gradle'],
            ['implementation.+${artifact_name}', None, r'gradle'],
            ['import.+${import_or_usage}', None, r'java$'],
            ['groupid>${group_id}<\/groupid>\s+<artifactid>${artifact_id}<\/artifactid>', re.IGNORECASE, r'.xml$', java_placeholders],
        ],
        'objective-c': [
            ['pod (?:`|\'|")${artifact_name}(?:`|\'|")', None, r'Podfile$'],
            ['${artifact_name}', None, r'Cartfile$'],
            ['(?:#(import|include) "${import_or_usage}"|import ${import_or_usage})', None, r'(?:m|h|swift)$'],
        ],
        'php': [
            ['require":[\S\W]*"${artifact_name}"', None, r'json$'],
            ['${import_or_usage}', None, r'php$'],
        ],
        'python': [
            ['install_requires=[\S\W]*(?:"|\')${artifact_name}(?:\[|\s+|~|=|>|<|!|"|\')', None, r'py$'],
            ['(?:(?:import|from).+${import_or_usage}|(?:INSTALLED_APPS|THIRD_PARTY_APPS|MIDDLEWARE_CLASSES).+(?:"|\')${import_or_usage})', None, r'py$'],
        ],
        'ruby': [
            ['gem (?:"|\')${artifact_name}(?:"|\')', None, r'Gemfile$'],
            ['require (?:"|\')${import_or_usage}(?:"|\')', None, r'rb$'],
        ],
        'scala': [
            ['import.+${import_or_usage}', None, r'(?:scala|sc)$'],
        ],
        'go': [
            ['${import_or_usage}', re.IGNORECASE, r'go$'],
        ],
        'markdown': [
            ['{artifact_name}', re.IGNORECASE, r'readme.md$'],
        ],

    }

    per_page = 100
    search_template = 'https://api.github.com/search/code?${params}+size:${from}..${to}+path%3A%2F+in%3Afile+extension%3Amd&page=${page}&s=indexed&o=desc&per_page=' + str(per_page)

    max_results = 1000

    number_duplicates = 0

    def as_params(self, search_string, languages):
        params_template = Template("q=%22${search_string}%22+${extensions_or_filenames}")

        # extensions = [extensions for lang, extensions in self.extensions_by_lang.items() if lang in languages][0]
        # if len(extensions) > 0:
        #     yield params_template.substitute({
        #         'search_string': search_string,
        #         'extensions_or_filenames': '+'.join(['extension:' + ext for ext in extensions])
        #     })

        filenames = [filenames for lang, filenames in self.filenames_by_lang.items() if lang in languages][0]
        if len(filenames) > 0:
            yield params_template.substitute({
                'search_string': search_string,
                'extensions_or_filenames': '+'.join(['filename:' + filename for filename in filenames])
            })

    def search_urls(self, library, page=1):
        languages = [lang.lower() for lang in library['languages'].split(',')]
        artifacts_names = [artifact.split(',')[0] for artifact in library['artifacts']]
        imports_usages = library['imports_usages']

        for search_string in artifacts_names + imports_usages:
            url_template = Template(self.search_template)
            for params in self.as_params(search_string, languages):
                yield url_template.substitute({
                    'params': params,
                    'page': page,
                    'from': int(self.size_from),
                    'to': int(self.size_to)
                })

    def start_requests(self):
        for library in self.libraries:
            self.repositories = {}
            self.exclude_pattern = {}
            library['matched'] = {} # Track to avoid unnecessary requests
            for url in self.search_urls(library):
                yield scrapy.Request(url=url, headers=self.get_headers(True), callback=self.parse,
                                     meta={'library': library, 'page': 1, 'from': self.size_from, 'to': self.size_to, 'per_page': self.per_page})

    repositories = {}

    exclude_pattern = {}

    def parse(self, response):
        page = response.meta['page']
        per_page = response.meta['per_page']
        max_pages = int(self.max_results / per_page)

        json_response = json.loads(response.text)

        # TODO (potentially) incomplete results are ignored for now
        # incomplete = False
        # if json_response['incomplete_results']:
        #     incomplete = True
            # print(page)
            # self.logger.warn('>>>>>> Incomplete results for %s', response.url)

        total_count = json_response['total_count']
        item_count = 0
        new_repos = 0

        if response.meta['from'] == response.meta['to'] and page == 1:
            # prevent from running excluded pattern
            for e in self.exclude_pattern.get(response.meta['from'], {}):
                if e in response.meta.get("splitter", ' '):
                    self.logger.info("already covered " + response.meta.get("splitter", '') + " - " + e)
                    return

            if total_count <= self.max_results:
                excluded = self.exclude_pattern.get(response.meta['from'], {})
                excluded[response.meta.get("splitter", ' ')] = 'excluded'
                self.exclude_pattern[response.meta['from']] = excluded

            if total_count == 0:
                return

        if len(json_response['items']) > 0:
            found_duplicate = False

            if total_count <= self.max_results or response.meta['from'] == response.meta['to']:
                # TODO extract to method
                for match in json_response['items']:
                    item_count += 1
                    repo_name = match['repository']['full_name']
                    file_name = match['name']
                    sha = match['sha']
                    identifier = repo_name + '_' + file_name + '_' + sha
                    marker = self.repositories.get(identifier, {})
                    if identifier in self.repositories:
                        if response.meta['from'] < response.meta['to'] and per_page == self.per_page:
                            if ('%d..%d' % (response.meta['from'], response.meta['to'])) in marker:
                                self.number_duplicates += 1
                                score = match['score']
                                # self.logger.info("duplicated: %d (page: %d) score: %d position:%d" % (self.number_duplicates, page, score, item_count))
                                found_duplicate = True

                            # copy = response.meta.copy()
                            # copy['page'] += 1
                            # next_page_url = response.url.replace('&page=' + str(page), '&page=1')
                            # new_per_page = 66 # random number
                            # copy['per_page'] = new_per_page
                            # next_page_url = next_page_url.replace('&per_page=%d' % per_page, '&per_page=%d' % new_per_page)
                            # yield response.follow(next_page_url, headers=self.get_headers(True), callback=self.parse, meta=copy)
                            #
                            # copy2 = response.meta.copy()
                            # copy2['page'] += 1
                            # next_page_url2 = response.url.replace('&page=' + str(page), '&page=1')
                            # new_per_page2 = 75  # random number
                            # copy2['per_page'] = new_per_page2
                            # next_page_url2 = next_page_url2.replace('&per_page=%d' % per_page, '&per_page=%d' % new_per_page2)
                            # yield response.follow(next_page_url2, headers=self.get_headers(True), callback=self.parse, meta=copy2)
                        continue

                    new_repos += 1
                    # if per_page != self.per_page:
                    #     self.logger.info("found new repo %d" % per_page)

                    marker['%d..%d' % (response.meta['from'], response.meta['to'])] = "FOUND"
                    self.repositories[identifier] = marker

                    response.meta['repo_name'] = repo_name
                    response.meta['path'] = match['path']

                    yield {
                        'library':      response.meta['library']['library'],
                        'repo_name':    repo_name,
                        'forked':       match['repository']['fork'],
                        'name':         file_name
                    }
                    # yield response.follow(url, headers=self.headers, callback=self.parse_contents, meta=response.meta)

                self.logger.info("%d / %d (%d) new repos found on page %d for range %d..%d, collected: %d" % (new_repos, item_count, total_count, page,
                                                                                                    response.meta['from'], response.meta['to'], len(self.repositories)))

                # Next page
                response.meta['page'] += 1
                if response.meta['page'] <= max_pages and (total_count <= self.max_results or response.meta['from'] == response.meta['to']):
                    next_page_url = response.url.replace('&page=' + str(page), '&page=' + str(response.meta['page']))
                    yield response.follow(next_page_url, headers=self.get_headers(False), callback=self.parse, meta=response.meta)

            if per_page != self.per_page:
                return

            if 'stop' not in response.meta and (total_count > self.max_results or found_duplicate):
                if item_count == 0 and response.meta['page'] == 1 and total_count < self.max_results:
                    return

                if response.meta['from'] < response.meta['to']:
                    if item_count == 0 and response.meta['page'] == 1 and total_count < self.max_results:
                        return
                    else:
                        # we need to rerun a smaller query
                        self.logger.info("SPLIT (size): total %d" % total_count)

                        response.meta['page'] = 1
                        old_from = response.meta['from']
                        old_to = response.meta['to']

                        size_to = int(old_from + (old_to - old_from) / 2)
                        next_page_url = response.url.replace('&page=' + str(page), '&page=1')
                        next_page_url = next_page_url.replace('+size:' + str(old_from) + ".." + str(old_to), '+size:' + str(old_from) + ".." + str(size_to))
                        copy1 = response.meta.copy()
                        copy1['to'] = size_to
                        yield response.follow(next_page_url, headers=self.get_headers(True), callback=self.parse, meta=copy1)

                        next_page_url2 = response.url.replace('&page=' + str(page), '&page=1')
                        next_page_url2 = next_page_url2.replace('+size:' + str(old_from) + ".." + str(old_to), '+size:' + str(size_to + 1) + ".." + str(old_to))
                        copy2 = response.meta.copy()
                        copy2['from'] = size_to + 1
                        yield response.follow(next_page_url2, headers=self.get_headers(True), callback=self.parse, meta=copy2)
                else:
                    # split even further if the files are of the same size
                    if page > 1:
                        return
                    page_url = response.url.replace('&page=' + str(page), '&page=1')

                    if total_count <= 2 * self.max_results:
                        self.logger.info("SPLIT (order): total %d" % total_count)
                        copy = response.meta.copy()
                        copy['stop'] = 'stop'
                        copy['page'] = 1
                        new_page_url = page_url.replace("&s=indexed&o=desc", "&s=indexed&o=asc")
                        yield response.follow(new_page_url, headers=self.get_headers(True), callback=self.parse, meta=copy)
                        # yield response.follow(page_url + new_page_url, headers=self.get_headers(True), callback=self.parse, meta=copy.copy())
                    else:
                        self.logger.info("SPLIT (query): total %d" % total_count)

                        splitter = response.meta.get("splitter", '')
                        excluded = self.exclude_pattern.get(response.meta['from'], {})
                        if len(splitter) <= 3:
                            for char in string.ascii_lowercase + string.digits:
                                match = re.search('q=%22.*%22', page_url)
                                found_str = match.group(0)
                                new_splitter = splitter + str(char)

                                exclude_new_splitter = False
                                for e in excluded:
                                    if e in new_splitter:
                                        self.logger.info("IGNORE " + new_splitter + " - " + e)
                                        exclude_new_splitter = True
                                        break
                                if exclude_new_splitter:
                                    continue

                                old_query = found_str
                                if len(splitter) > 0:
                                    old_query = found_str + "+" + splitter

                                new_query = found_str + "+" + new_splitter
                                next_page_url = page_url.replace(old_query, new_query)
                                copy = response.meta.copy()
                                copy["splitter"] = new_splitter
                                copy['page'] = 1
                                self.logger.info(next_page_url)
                                yield response.follow(next_page_url, headers=self.get_headers(True), callback=self.parse, meta=copy)

    def parse_contents(self, response):
        matched = response.meta['library']['matched']
        repo_name = response.meta['repo_name']
        if matched.get(repo_name):
            return

        library = response.meta['library']
        path = response.meta['path']
        json_response = json.loads(response.text)
        content = base64.b64decode(json_response['content'])
        languages = [lang.lower() for lang in library['languages'].split(',')]

        artifacts = [artifact.split(',')[0] for artifact in library['artifacts']]
        # import_or_usage fallbacks into artifact_names
        imports = library['imports_usages'] if len(library['imports_usages']) > 0 else artifacts
        placeholders_pairs = [(artifact, import_usage) for artifact in artifacts for import_usage in imports]
        placeholders_set = [{
            'artifact_name': re.escape(artifact),
            'import_or_usage': re.escape(import_usage),
        } for artifact, import_usage in placeholders_pairs]

        templates = [templates for lang, templates in self.regexp_templates_by_lang.items() if lang in languages][0]
        searches_memo = dict()

        self.logger.debug('Placeholders %s', str(placeholders_set))
        for placeholders in placeholders_set:
            if matched.get(repo_name):
                break

            for template in templates:
                if matched.get(repo_name):
                    break

                content_template_regexp = template
                flags = 0
                path_regexp = ''
                augment_placeholders_fn = None

                if type(template) is list:
                    length = len(template)
                    content_template_regexp = template[0]
                    flags = template[1] if length > 1 and template[1] else 0
                    path_regexp = template[2] if length > 2 else ''
                    augment_placeholders_fn = template[3] if length > 3 else None

                    if path_regexp and not re.search(path_regexp, path, re.MULTILINE):
                        continue

                    if augment_placeholders_fn:
                        placeholders = augment_placeholders_fn(placeholders)

                template = Template(content_template_regexp)
                regexp = r'' + template.substitute(placeholders)

                if searches_memo.get(regexp):
                    continue

                searches_memo[regexp] = True

                self.logger.debug('RE Searching %s in %s', regexp, path)
                # Some files will arrive as non utf-8 (specially txt files), lets ignore,
                # the output seems to be ok for our purposes
                if re.search(regexp, str(content, encoding='utf-8', errors='ignore'), flags):
                    matched[repo_name] = True
                    self.logger.info('Matched %s in %s', library['library'], repo_name)
                    toggled_repo = { key: None for key in self.csv_fieldnames }
                    toggled_repo['repo_name'] = repo_name
                    toggled_repo['path'] = path
                    toggled_repo['library'] = library['library']
                    toggled_repo['library_language'] = library['languages']
                    yield toggled_repo
