# ParseCVF

Trivial spider that grabs links and abstracts on CVF Open Access.

## Prerequisites

* Python3
* requests
* urllib
* colorama

## Example Usage

```
python3 Scripts/make_db.py --link http://openaccess.thecvf.com/CVPR2017.py --conf CVPR --year 2017
```
would create a SQLite database at path CVPR.2017.db at cwd.

Use any database interface to read. I recommend [DB Browser](https://sqlitebrowser.org/), which is cross-platform and along with a SQLite terminal.

## Known issues
* Database creation is currently done in a brute-force way.
* Detail paths with `.` in filename always cause a 404. This is also verifiable from browsers, so this is not a bug of this spider.