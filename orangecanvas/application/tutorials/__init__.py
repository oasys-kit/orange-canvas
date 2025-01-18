"""
Orange Canvas Tutorial schemes

"""
import os
import io
import logging
import types
import collections
from itertools import chain

import six

#import pkg_resources
import importlib_metadata, importlib_resources

log = logging.getLogger(__name__)


def list_schemes(package):
    """Return a list of scheme tutorials.
    """
    #resources = pkg_resources.resource_listdir(package.__name__, ".")
    resources = importlib_resources.contents(package.__name__)
    return [resource for resource in resources if not importlib_resources.is_resource(package.__name__, resource)]

    resources = filter(is_ows, resources)
    return sorted(resources)


def is_ows(filename):
    return filename.endswith(".ows")


def default_entry_point():
    #dist = pkg_resources.get_distribution("Orange")
    #ep = pkg_resources.EntryPoint("Orange Canvas", __name__, dist=dist)

    dist = importlib_metadata.distribution("Orange")
    ep = importlib_metadata.EntryPoint(name="Orange Canvas", value=__name__, group=dist.name)

    return ep


def tutorial_entry_points():
    """Return an iterator over all tutorials.
    """
    default = default_entry_point()
    #return chain([default], pkg_resources.iter_entry_points("orange.widgets.tutorials"))
    return chain([default], importlib_metadata.entry_points(group="orange.widgets.tutorials"))

def tutorials():
    """Return all known tutorials.
    """
    all_tutorials = []
    for ep in tutorial_entry_points():
        tutorials = None
        try:
            tutorials = ep.load()
        #except pkg_resources.DistributionNotFound as ex:
        except importlib_metadata.PackageNotFoundError as ex:
            log.warning("Could not load tutorials from %r (%r)", ep.group, ex)
            continue
        except ImportError:
            log.error("Could not load tutorials from %r", ep.group, exc_info=True)
            continue
        except Exception:
            log.error("Could not load tutorials from %r", ep.group, exc_info=True)
            continue

        if isinstance(tutorials, types.ModuleType):
            package = tutorials
            tutorials = list_schemes(tutorials)
            #tutorials = [Tutorial(t, package, ep.dist) for t in tutorials]
            tutorials = [Tutorial(t, package, ep.group) for t in tutorials]
        elif isinstance(tutorials, (types.FunctionType, types.MethodType)):
            try:
                tutorials = tutorials()
            except Exception as ex:
                log.error("A callable entry point (%r) raised an "
                          "unexpected error.",
                          ex, exc_info=True)
                continue
            #tutorials = [Tutorial(t, package=None, distribution=ep.dist) for t in tutorials]
            tutorials = [Tutorial(t, package=None, distribution=ep.group) for t in tutorials]
        all_tutorials.extend(tutorials)

    return all_tutorials


class Tutorial(object):
    def __init__(self, resource, package=None, distribution=None):
        self.resource = resource
        self.package = package
        self.distribution = distribution

    def abspath(self):
        """Return absolute filename for the scheme if possible else
        raise an ValueError.

        """
        if self.package is not None:
            #return pkg_resources.resource_filename(self.package.__name__, self.resource)
            ref = importlib_resources.files(self.package.__name__).joinpath(self.resource)
            with importlib_resources.as_file(ref) as resource_filename: return str(resource_filename)

        elif isinstance(self.resource, six.string_types):
            if os.path.isabs(self.resource):
                return self.resource

        raise ValueError("cannot resolve resource to an absolute name")

    def stream(self):
        """Return the tutorial file as an open stream.
        """
        if self.package is not None:
            #return pkg_resources.resource_stream(self.package.__name__, self.resource)
            ref = importlib_resources.files(self.package.__name__).joinpath(self.resource)
            return ref.open('rb')

        elif isinstance(self.resource, six.string_types):
            if os.path.isabs(self.resource) and os.path.exists(self.resource):
                return open(self.resource, "rb")

        raise ValueError
