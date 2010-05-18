{application, erls3,
 [{description, "Access to S3 via REST interface"},
  {author, "Eric Cestari"},
  {vsn, "v1.8.1"},
  {modules, [ erls3, erls3server, erls3sup, erls3util, console_events]},
  {mod, {erls3,[]}},
  {registered, [erls3sup]},
  {applications, [kernel, stdlib, crypto, xmerl, ibrowse]}
 ]}.
