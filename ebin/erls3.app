{application, erls3,
 [{description, "Access to S3 via REST interface"},
  {author, "Eric Cestari"},
  {vsn, "0.9"},
  {modules, [ erls3, erls3server, erls3sup, erls3test, erls3util]},
  {mod, {erls3,[]}},
  {registered, [erls3sup]},
  {applications, [kernel, stdlib, crypto, xmerl, ibrowse]}
 ]}.
