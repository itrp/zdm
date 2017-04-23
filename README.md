# Zero Downtime Migrator

Minimal code to migrate big tables in mysql, mariadb or aurora with zero downtime of the systems.
Only works with tables that have an auto increment primary key column named `id`.

Read [Facebook's OCS](https://www.facebook.com/note.php?note_id=430801045932) commentary.
Instead of using outfiles we follow [lhm](https://github.com/soundcloud/lhm)'s approach.

The code is the readme. If you donot grok the code then you really should not use this.

See also documentation from mysql on [Online Status for DDL Operations](https://dev.mysql.com/doc/refman/5.7/en/innodb-create-index-overview.html#innodb-online-ddl-summary-grid).

Install
=======

```
gem install zdm
```

[![Build Status](https://travis-ci.org/itrp/zdm.png)](https://travis-ci.org/itrp/zdm)