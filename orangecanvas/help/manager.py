"""

"""
import sys
import os
import string
import itertools
import logging
import email

from distutils.version import StrictVersion

from operator import itemgetter

#import pkg_resources
import importlib_metadata

from .provider import IntersphinxHelpProvider

from PyQt5.QtCore import QObject, QUrl

log = logging.getLogger(__name__)


class HelpManager(QObject):
    def __init__(self, parent=None):
        QObject.__init__(self, parent)
        self._registry = None
        self._initialized = False
        self._providers = {}

    def set_registry(self, registry):
        """
        Set the widget registry for which the manager should
        provide help.

        """
        if self._registry is not registry:
            self._registry = registry
            self._initialized = False
            self.initialize()

    def registry(self):
        """
        Return the previously set with set_registry.
        """
        return self._registry

    def initialize(self):
        if self._initialized:
            return

        reg = self._registry

        try:    all_projects = set(desc.project_name for desc in reg.widgets() if desc.project_name is not None)
        except: all_projects = set(desc.name for desc in reg.widgets() if desc.name is not None)

        providers = []
        for project in set(all_projects) - set(self._providers.keys()):
            provider = None
            #try:
            #    dist = pkg_resources.get_distribution(project)
            #except pkg_resources.ResolutionError:
            try:
                dist = importlib_metadata.distribution(project)
            except importlib_metadata.PackageNotFoundError:
                log.exception("Could not get distribution for '%s'", project)
            else:
                try:
                    provider = get_help_provider_for_distribution(dist)
                except Exception:
                    log.exception("Error while initializing help "
                                  "provider for %r", project)

            if provider:
                providers.append((project, provider))
                provider.setParent(self)

        self._providers.update(dict(providers))
        self._initialized = True

    def get_help(self, url):
        """
        """
        self.initialize()
        if url.scheme() == "help" and url.authority() == "search":
            return self.search(qurl_query_items(url))
        else:
            return url

    def description_by_id(self, desc_id):
        reg = self._registry
        return get_by_id(reg, desc_id)

    def search(self, query):
        self.initialize()

        if isinstance(query, QUrl):
            query = qurl_query_items(query)

        query = dict(query)
        desc_id = query["id"]
        desc = self.description_by_id(desc_id)

        provider = None
        try:
            if desc.project_name: provider = self._providers.get(desc.project_name)
        except:
            if desc.name: provider = self._providers.get(desc.name)

        # TODO: Ensure initialization of the provider
        if provider:
            return provider.search(desc)
        else:
            raise KeyError(desc_id)


def get_by_id(registry, descriptor_id):
    for desc in registry.widgets():
        if desc.id == descriptor_id:
            return desc

    raise KeyError(descriptor_id)

import urllib

def qurl_query_items(url):
    if not url.hasQuery():
        return []
    querystr = url.query()
    return urllib.parse.parse_qsl(querystr)


def get_help_provider_for_description(desc):
    if desc.project_name:
        #dist = pkg_resources.get_distribution(desc.project_name)
        try:    dist = importlib_metadata.distribution(desc.project_name)
        except: dist = importlib_metadata.distribution(desc.name)
        return get_help_provider_for_distribution(dist)


def is_develop_egg(dist):
    """
    Is the distribution installed in development mode (setup.py develop)
    """
    meta_provider = dist._provider
    egg_info_dir = os.path.dirname(meta_provider.egg_info)
    #egg_name = pkg_resources.to_filename(dist.project_name)
    try:    dist = importlib_metadata.distribution(dist.project_name)
    except: dist = importlib_metadata.distribution(dist.name)
    # Manually convert the project name to a filename
    egg_name = dist.metadata['Name'].replace('-', '_')

    return meta_provider.egg_info.endswith(egg_name + ".egg-info") \
           and os.path.exists(os.path.join(egg_info_dir, "setup.py"))


def left_trim_lines(lines):
    """
    Remove all unnecessary leading space from lines.
    """
    lines_striped = zip(lines[1:], map(str.lstrip, lines[1:]))
    lines_striped = filter(itemgetter(1), lines_striped)
    indent = min([len(line) - len(striped) \
                  for line, striped in lines_striped] + [sys.maxsize])

    if indent < sys.maxsize:
        return [line[indent:] for line in lines]
    else:
        return list(lines)


def trim_trailing_lines(lines):
    """
    Trim trailing blank lines.
    """
    lines = list(lines)
    while lines and not lines[-1]:
        lines.pop(-1)
    return lines


def trim_leading_lines(lines):
    """
    Trim leading blank lines.
    """
    lines = list(lines)
    while lines and not lines[0]:
        lines.pop(0)
    return lines


def trim(string):
    """
    Trim a string in PEP-256 compatible way
    """
    lines = string.expandtabs().splitlines()

    lines = list(map(str.lstrip, lines[:1])) + left_trim_lines(lines[1:])

    return  "\n".join(trim_leading_lines(trim_trailing_lines(lines)))


# Fields allowing multiple use (from PEP-0345)
MULTIPLE_KEYS = ["Platform", "Supported-Platform", "Classifier",
                 "Requires-Dist", "Provides-Dist", "Obsoletes-Dist",
                 "Project-URL"]


def parse_meta(contents):
    message = email.message_from_string(contents)
    meta = {}
    for key in set(message.keys()):
        if key in MULTIPLE_KEYS:
            meta[key] = message.get_all(key)
        else:
            meta[key] = message.get(key)

    if len(meta.keys()) == 0: return {}

    version = StrictVersion(meta["Metadata-Version"])

    if version >= StrictVersion("1.3") and "Description" not in meta:
        desc = message.get_payload()
        if desc:
            meta["Description"] = desc
    return meta


def get_meta_entry(dist, name):
    """
    Get the contents of the named entry from the distributions PKG-INFO file
    """
    meta = get_dist_meta(dist)
    return meta.get(name)


def get_dist_url(dist):
    """
    Return the 'url' of the distribution (as passed to setup function)
    """
    return get_meta_entry(dist, "Home-page")


def get_dist_meta(dist):
    '''
    if dist.has_metadata("PKG-INFO"):
        # egg-info
        contents = dist.get_metadata("PKG-INFO")
    elif dist.has_metadata("METADATA"):
        # dist-info
        contents = dist.get_metadata("METADATA")
    else:
        contents = None
    '''
    try:
        contents = str(dist.metadata)
    except:
        contents = ""
        for key in dist.metadata.json.keys():
            new_key = str(key)
            if not "_" in new_key: new_key = new_key[0].upper() + new_key[1:]
            else:
                if key == "author_email": new_key = 'Author-email'
                elif key == "download_url": new_key = 'Download-URL'
                else:
                    tokens = new_key.split("_")
                    new_key = ""
                    for t in tokens: new_key += t[0].upper() + t[1:] + "-"
                    new_key = new_key[:-1]
            data = dist.metadata.json[key]
            if type(data) == str: contents += new_key + ": " + data + "\n"
            elif type(data) == list:
                for item in data: contents += new_key + ": " + item + "\n"

    if contents is not None:
        return parse_meta(contents)
    else:
        return {}

# Note this method is designed for pkg_resources, but it is never called
# so I won't refactor it for importlib
def create_intersphinx_provider(entry_point):
    locations = entry_point.load()
    dist = entry_point.dist

    replacements = {"PROJECT_NAME": dist.project_name,
                    "PROJECT_NAME_LOWER": dist.project_name.lower(),
                    "PROJECT_VERSION": dist.version}

    try:
        replacements["URL"] = get_dist_url(dist)
    except KeyError:
        pass

    formatter = string.Formatter()

    for target, inventory in locations:
        # Extract all format fields
        format_iter = formatter.parse(target)
        if inventory:
            format_iter = itertools.chain(format_iter,
                                          formatter.parse(inventory))
        fields = map(itemgetter(1), format_iter)
        fields = filter(None, set(fields))

        if "DEVELOP_ROOT" in fields:
            if not is_develop_egg(dist):
                # skip the location
                continue
            target = formatter.format(target, DEVELOP_ROOT=dist.location)

            if os.path.exists(target) and \
                    os.path.exists(os.path.join(target, "objects.inv")):
                return IntersphinxHelpProvider(target=target)
            else:
                continue
        elif fields:
            try:
                target = formatter.format(target, **replacements)
                if inventory:
                    inventory = formatter.format(inventory, **replacements)
            except KeyError:
                log.exception("Error while formating intersphinx mapping "
                              "'%s', '%s'." % (target, inventory))
                continue

            return IntersphinxHelpProvider(target=target, inventory=inventory)
        else:
            return IntersphinxHelpProvider(target=target, inventory=inventory)

    return None


_providers = {"intersphinx": create_intersphinx_provider}


def get_help_provider_for_distribution(dist):
    # entry_points = dist.get_entry_map().get("orange.canvas.help", {})
    entry_points = {ep.value : ep for ep in dist.entry_points}.get("orange.canvas.help", {})

    provider = None
    for name, entry_point in entry_points.items():
        create = _providers.get(name, None)
        if create:
            try:
                provider = create(entry_point)
            #except pkg_resources.DistributionNotFound as err:
            #    log.warning("Unsatisfied dependencies (%r)", err)
            #    continue
            except Exception as err:
                #log.exception("Exception")
                log.warning("Exception (%r)", err)
                continue
            if provider:
                log.info("Created %s provider for %s",
                         type(provider), dist)
                break

    return provider
