This is just an experiment to see if a set of arbitrary chunks can be
identified within a seed file with reasonable performance.  Looks like
they can!

* I can take a 1GiB file and break it up into 16134 chunks of 64 KiB each -
  this takes 4 seconds.
* I can then search through that original 1GiB file, trying to find these
  chunks - this takes 10 seconds.

This proves the feasibility of a zsync clone, but where the server is allowed
to chunk its file in any way it thinks best.  eg. We could insert breaks at the
file boundaries within a tarball.  Or, if you have a collection of old versions
of the file that exist commonly in the wild, you could find the places where
the new version differs from them, and place breaks at exactly those points.

