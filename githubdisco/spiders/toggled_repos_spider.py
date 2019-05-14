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

def java_placeholders(_placeholders):
    placeholders = dict(_placeholders)
    group_id, artifact_id = placeholders['artifact_name'].split(',', 1)[0].split(':', 1)
    placeholders['group_id'] = group_id
    placeholders['artifact_id'] = artifact_id
    return placeholders

class ToggledReposSpider(scrapy.Spider):
    name = "toggled_repos"

    csv_fieldnames = ['repo_name', 'path', 'language', 'size_bytes', 'library', 'library_language', 'last_commit_ts', 'forked_from']

    headers = {
        'Authorization': 'Bearer ' + os.environ['AUTH_TOKEN'],
    }

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
    }

    filenames_by_lang = {
        'objective-c':  ['Podfile', 'Cartfile'], # also: Swift
        'ruby':         ['Gemfile'],
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
    }

    search_template = 'https://api.github.com/search/code?${params}&page=${page}&per_page=50'
    per_page = 50

    def as_params(self, search_string, languages):
        params_template = Template("q=%22${search_string}%22+in:file+${extensions_or_filenames}")

        extensions = [extensions for lang, extensions in self.extensions_by_lang.items() if lang in languages][0]
        if len(extensions) > 0:
            yield params_template.substitute({
                'search_string': search_string,
                'extensions_or_filenames': '+'.join(['extension:' + ext for ext in extensions])
            })

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
                })

    def start_requests(self):
        for library in self.libraries:
            library['matched'] = {} # Track to avoid unnecessary requests
            for url in self.search_urls(library):
                yield scrapy.Request(url=url, headers=self.headers, callback=self.parse, meta={ 'library': library, 'page': 1 })

    def parse(self, response):
        page = response.meta['page']
        json_response = json.loads(response.text)
        if (json_response['incomplete_results']):
            self.logger.warn('>>>>>> Incomplete results for %s', response.url)

        if len(json_response['items']) > 0:
            for match in json_response['items']:
                response.meta['repo_name'] = match['repository']['full_name']
                response.meta['path'] = match['path']
                url = match['git_url']
                yield response.follow(url, headers=self.headers, callback=self.parse_contents, meta=response.meta)

            # Next page
            response.meta['page'] += 1
            next_page_url = response.url.replace('&page=' + str(page), '&page=' + str(response.meta['page']))
            yield response.follow(next_page_url, headers=self.headers, callback=self.parse, meta=response.meta)
        elif page == 1:
            self.logger.warn('!! Found no matches for %s', response.url)

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
