# Zero Downtime Migrator

Minimal code to migrate big tables in mysql, mariadb or aurora with zero downtime of the systems.
Only works with tables that have an auto increment primary key column named `id`.

Read [Facebook's OCS](https://www.facebook.com/note.php?note_id=430801045932) commentary.
Instead of using outfiles we follow [lhm](https://github.com/soundcloud/lhm)'s approach.

The code is the readme. If you donot grok the code then you really should not use this.

Install
=======

```
gem install zdm
```
