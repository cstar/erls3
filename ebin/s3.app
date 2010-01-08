{application, s3,
 [{description, "Access to S3 via REST interface"},
  {author, "Eric Cestari"},
  {vsn, "0.7"},
  {modules, [ s3, s3server, s3sup, s3test, s3util]},
  {mod, {s3,[]}},
  {registered, [s3sup]},
  {applications, [kernel, stdlib, crypto, xmerl]}
 ]}.
