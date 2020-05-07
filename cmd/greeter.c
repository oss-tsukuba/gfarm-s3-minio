#include <sys/types.h>
#include <sys/stat.h>
#include <sys/time.h>

#include <assert.h>
#include <errno.h>
#include <fcntl.h>
#include <libgen.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>

#include <gfarm/gfarm.h>

#include "greeter.h"

#include "gfarm/gfs_profile.h"
#include "gfarm/host.h"
#include "gfarm/config.h"
#include "gfarm/gfarm_path.h"
#include "gfarm/gfs_pio.h"
#include "gfarm/context.h"
#include "gfarm/gfs_rdma.h"

#include "gfutil/gfutil.h"
#include "gfutil/timer.h"

static char *program_name_gfexport = "gfexport";
static int gfexport_main(char *, int);

static char *program_name_gfreg = "gfreg";
static int writemode = 0;
static int gfreg_main(int, char *);

int
greet(const char *name, int year, char *out)
{
	int n;

	n = sprintf(out, "Greetings, %s from %d! We come in peace :)", name, year);

	return n;
}

int
gfreg_d(int d)
{
	gfreg_main(d, "foo.dat");
	return 0;
}

int
qqq(int d)
{
	gfexport_main("foo.dat", d);
	return 0;
}

static gfarm_error_t
gfexport_d(char *gfarm_url, char *host, int d,
	gfarm_off_t off, gfarm_off_t size)
{
	gfarm_error_t e, e2;
	struct gfs_stat st;
	GFS_File gf;

	e = gfs_pio_open(gfarm_url, GFARM_FILE_RDONLY, &gf);
	if (e != GFARM_ERR_NO_ERROR)
		return e;
	e = gfs_fstat(gf, &st);
	if (e != GFARM_ERR_NO_ERROR)
		goto err;
	if (size < 0)
		size = st.st_size;
	gfs_stat_free(&st);
	/* XXX FIXME: INTERNAL FUNCTION SHOULD NOT BE USED */
	e = gfs_pio_internal_set_view_section(gf, host);
	if (e == GFARM_ERR_NO_ERROR)
		e = gfs_pio_recvfile(gf, off, d, 0, size, NULL);
err:
	e2 = gfs_pio_close(gf);
	return e != GFARM_ERR_NO_ERROR ? e : e2;
}

static int
gfexport_main(path, d)
char *path;
int d;
{
	gfarm_error_t e;
	char *url, *realpath, *hostname = NULL;
	gfarm_off_t off = 0, size = -1;

	e = gfarm_initialize(0, NULL);
	if (e != GFARM_ERR_NO_ERROR) {
		fprintf(stderr, "%s: gfarm_initialize(): %s\n", program_name_gfexport,
		    gfarm_error_string(e));
		return -1;
	}

	e = gfarm_realpath_by_gfarm2fs(path, &realpath);
	if (e == GFARM_ERR_NO_ERROR) {
		url = realpath;
	}
	else {
		url = path;
		realpath = NULL;
	}

	e = gfexport_d(url, hostname, d, off, size);
	if (e != GFARM_ERR_NO_ERROR) {
		fprintf(stderr, "%s: %s: %s\n", program_name_gfexport, path,
		    gfarm_error_string(e));
		return -1;
	}
	free(realpath);

	e = gfarm_terminate();
	if (e != GFARM_ERR_NO_ERROR) {
		fprintf(stderr, "%s: gfarm_terminate(): %s\n", program_name_gfexport,
		    gfarm_error_string(e));
		return -1;
	}

	return 0;
}

static gfarm_error_t
gfpio_write(GFS_File w_gf, gfarm_off_t w_off, int r_fd, gfarm_off_t r_off,
	gfarm_off_t len, gfarm_off_t *sentp)
{
	gfarm_error_t e = GFARM_ERR_NO_ERROR;
#define BUF_SIZE 0x100000
	char buffer[BUF_SIZE];
	int rlen, off;
	int rrv, rv;

	if (len < 0)
		len = (gfarm_off_t)(1LL << 62);
	for (; len > 0;) {
		rlen = len < BUF_SIZE ?  len : BUF_SIZE;
		rrv = pread(r_fd, buffer, rlen, r_off);
		if (rrv == 0)
			break;
		if (rrv == -1) {
			e = gfarm_errno_to_error(errno);
			break;
		}
		for (off = 0; rrv > 0; ) {
			e = gfs_pio_pwrite(w_gf, buffer + off, rrv, w_off, &rv);
			if (e != GFARM_ERR_NO_ERROR) {
				goto err;
			}
			r_off += rv;
			w_off += rv;
			off += rv;
			len -= rv;
			rrv -= rv;
		}
	}
err:
	if (sentp)
		*sentp = r_off;
	return (e);
}

static gfarm_error_t
gfimport_to(int ifd, char *gfarm_url, int mode,
	char *host, gfarm_off_t off, gfarm_off_t size)
{
	gfarm_error_t e, e2;
	GFS_File gf;
	gfarm_timerval_t t1, t2, t3, t4, t5;
	int flags;

	GFARM_TIMEVAL_FIX_INITIALIZE_WARNING(t1);
	GFARM_TIMEVAL_FIX_INITIALIZE_WARNING(t2);
	GFARM_TIMEVAL_FIX_INITIALIZE_WARNING(t3);
	GFARM_TIMEVAL_FIX_INITIALIZE_WARNING(t4);
	GFARM_TIMEVAL_FIX_INITIALIZE_WARNING(t5);

	gfs_profile(gfarm_gettimerval(&t1));
	if (off > 0)
		flags = GFARM_FILE_WRONLY;
	else
		flags = GFARM_FILE_WRONLY|GFARM_FILE_TRUNC;
	e = gfs_pio_create(gfarm_url, flags, mode, &gf);
	if (e != GFARM_ERR_NO_ERROR) {
		fprintf(stderr, "%s: %s\n", gfarm_url, gfarm_error_string(e));
		return (e);
	}
	gfs_profile(gfarm_gettimerval(&t2));
	/* XXX FIXME: INTERNAL FUNCTION SHOULD NOT BE USED */
	e = gfs_pio_internal_set_view_section(gf, host);
	if (e != GFARM_ERR_NO_ERROR) {
		fprintf(stderr, "%s: %s\n", gfarm_url, gfarm_error_string(e));
		if ((flags & GFARM_FILE_TRUNC) != 0)
			gfs_unlink(gfarm_url);
		goto close;
	}
	gfs_profile(gfarm_gettimerval(&t3));

	if (writemode)
		e = gfpio_write(gf, off, ifd, 0, size, NULL);
	else
		e = gfs_pio_sendfile(gf, off, ifd, 0, size, NULL);
	if (e != GFARM_ERR_NO_ERROR)
		fprintf(stderr, "writing to %s: %s\n", gfarm_url,
			gfarm_error_string(e));
	gfs_profile(gfarm_gettimerval(&t4));
 close:
	e2 = gfs_pio_close(gf);
	if (e2 != GFARM_ERR_NO_ERROR)
		fprintf(stderr, "closing %s: %s\n", gfarm_url,
			gfarm_error_string(e2));
	gfs_profile(gfarm_gettimerval(&t5));
	gfs_profile(fprintf(stderr,
				"create %g, view %g, import %g, close %g\n",
				gfarm_timerval_sub(&t2, &t1),
				gfarm_timerval_sub(&t3, &t2),
				gfarm_timerval_sub(&t4, &t3),
				gfarm_timerval_sub(&t5, &t4)));

	return (e != GFARM_ERR_NO_ERROR ? e : e2);
}

static gfarm_error_t
gfimport_from_to_d(int ifd, char *gfarm_url,
	char *host, gfarm_off_t off, gfarm_off_t size)
{
	gfarm_error_t e;
	struct stat st;

	st.st_mode = 0600;
	e = gfimport_to(ifd, gfarm_url, st.st_mode & 0777, host, off, size);
	if (ifd != STDIN_FILENO)
		close(ifd);
	return (e);
}

static int
gfreg_main(int d, char *gfarm_url)
{
	gfarm_error_t e;
	int status = 0;
	char *path = NULL;

	e = gfarm_initialize(0, NULL);
	if (e != GFARM_ERR_NO_ERROR) {
		fprintf(stderr, "%s: %s\n", program_name_gfreg,
			gfarm_error_string(e));
		return -1;
	}

	if (!writemode)
		gfs_ib_rdma_disable();

	e = gfarm_realpath_by_gfarm2fs(gfarm_url, &path);
	if (e == GFARM_ERR_NO_ERROR)
		gfarm_url = path;
	e = gfimport_from_to_d(d, gfarm_url, NULL, 0, -1);
	if (e != GFARM_ERR_NO_ERROR)
		status = 1;
	free(path);

	e = gfarm_terminate();
	if (e != GFARM_ERR_NO_ERROR) {
		fprintf(stderr, "%s: %s\n", program_name_gfreg,
			gfarm_error_string(e));
		status = 1;
	}
	return status;
}
