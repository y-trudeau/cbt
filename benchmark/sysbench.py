import subprocess
import common
import settings
import monitoring
import os
import time

from benchmark import Benchmark

class MysqlSysBench(Benchmark):

    def __init__(self, cluster, config):
        super(MysqlSysBench, self).__init__(cluster, config)

      
        self.cmd_path = config.get('cmd_path', '/usr/bin/sysbench')
        self.pool_profile = config.get('pool_profile', 'default')

--num-threads=16 --mysql-socket=/var/run/mysqld/mysqld.sock
 --mysql-database=sbtest --mysql-user=root --test=/usr/share/doc/sysbench/tests/db/oltp.lua --oltp-table-size=50000000  --oltp-test-mode=complex --mysql-engine=innodb --db-driver=mysql --report-interval=60 --max-requests=0 --max-time=3600

        self.threads = config.get('num-threads', 1)
        self.total_procs = self.threads * len(settings.getnodes('clients').split(','))
        
        self.mysql_socket =  str(config.get('mysql-socket', '/var/run/mysqld/mysqld.sock'))
        self.mysql_user =  str(config.get('mysql-user', 'sbtest'))
        self.mysql_pass = str(config.get('mysql-password', 'sbtest'))
        self.mysql_database = str(config.get('mysql-database', 'sbtest'))
        self.mysql_engine = str(config.get('mysql-engine', 'innodb'))
        
        self.test_path = str(config.get('test-path', '/usr/share/doc/sysbench/tests/db/oltp.lua'))
        self.oltp_table_count = config.get('oltp-table-count',1)
        self.oltp_table_size = config.get('oltp-table-size',10000)
        self.oltp_read_only = str(config.get('oltp-read-only','off'))
        self.oltp_point_selects = config.get('oltp-point-select',10)
        self.oltp_range_size = config.get('oltp-range-size',100)
        self.oltp_simple_ranges = config.get('oltp-simple-ranges',1)
        self.oltp_sum_ranges = config.get('oltp-sum-ranges',1)
        self.oltp_order_ranges = config.get('oltp-order-ranges',1)
        self.oltp_distinct_ranges = config.get('oltp-distinct-ranges',1)
        self.oltp_index_updates = config.get('oltp-index-updates',1)
        self.oltp_non_index_updates = config.get('oltp-non-index-updates',0)
   
        self.max_time = config.get('max-time', 3600)
        self.report_time = config.get('report-time', 3600)
        
        self.mycnf_innodb_buffer_pool_size = config.get('mycnf-innodb-buffer-pool-size', 128)
        self.mycnf_innodb_log_file_size = config.get('mycnf-innodb-log-file-size', 128)
        self.mycnf_innodb_log_buffer_size = config.get('mycnf-innodb-log-file-size', 8)
        self.mycnf_innodb_read_io_threads = config.get('mycnf-innodb-read-io-threads', 4)
        self.mycnf_innodb_write_io_threads = config.get('mycnf-innodb-write-io-threads', 4)
        self.mycnf_innodb_doublewrite = config.get('mycnf-innodb-doublewrite', 1)
        self.mycnf_innodb_file_format = str(config.get('mycnf-innodb_file_format', 'Antelope'))
        self.mycnf_innodb_flush_method = str(config.get('mycnf-innodb_flush_method', 'O_DIRECT'))
        self.mycnf_innodb_flush_log_at_trx_commit = config.get('mycnf-innodb_flush_log_at_trx_commit', 1)
        self.mycnf_innodb_flush_neighbors = config.get('mycnf-innodb_flush_neighbors', 1)

        
        self.vol_size = config.get('vol_size', 65536)
        self.vol_order = config.get('vol_order', 22)
        self.random_distribution = config.get('random_distribution', None)
        self.rbdadd_mons = config.get('rbdadd_mons')
        self.rbdadd_options = config.get('rbdadd_options', 'share')
        self.client_ra = config.get('client_ra', 128)
        self.poolname = "cbt-mysqlsysbench"

        self.run_dir = '%s/mysqlsysbench/ts-%09d' % (self.run_dir, int(time.time()))
        self.out_dir = '%s/mysqlsysbench/ts-%s' % (self.archive_dir, int(time.time()))

        # Make the file names string
        #self.names = ''
        #for i in xrange(self.concurrent_procs):
        #    self.names += '--name=%s/mnt/cbt-kernelrbdfio-`hostname -s`/cbt-kernelrbdfio-%d ' % (self.cluster.tmp_dir, i)

    def exists(self):
        if os.path.exists(self.out_dir):
            print 'Skipping existing test in %s.' % self.out_dir
            return True
        return False

    def initialize(self): 
        super(MysqlSysBench, self).initialize()

        print 'Running scrub monitoring.'
        monitoring.start("%s/scrub_monitoring" % self.run_dir)
        self.cluster.check_scrub()
        monitoring.stop()

        print 'Pausing for 60s for idle monitoring.'
        monitoring.start("%s/idle_monitoring" % self.run_dir)
        time.sleep(60)
        monitoring.stop()

        common.sync_files('%s/*' % self.run_dir, self.out_dir)

        self.mkimages()
 
        # Create the run directory
        common.make_remote_dir(self.run_dir)

        # populate the fio files
        print 'Attempting to populating fio files...'
        pre_cmd = 'sudo %s --ioengine=%s --rw=write --numjobs=%s --bs=4M --size %dM %s > /dev/null' % (self.cmd_path, self.ioengine, self.numjobs, self.vol_size*0.9, self.names)
        common.pdsh(settings.getnodes('clients'), pre_cmd).communicate()

        return True


    def run(self):
        super(MysqlSysBench, self).run()

        # Set client readahead
        self.set_client_param('read_ahead_kb', self.client_ra)

        # We'll always drop caches for rados bench
        self.dropcaches()

        monitoring.start(self.run_dir)

        # Run the backfill testing thread if requested
        if 'recovery_test' in self.cluster.config:
            recovery_callback = self.recovery_callback
            self.cluster.create_recovery_test(self.run_dir, recovery_callback)

        time.sleep(5)
        out_file = '%s/output' % self.run_dir
        fio_cmd = 'sudo %s' % (self.cmd_path_full)
        fio_cmd += ' --rw=%s' % self.mode
        if (self.mode == 'readwrite' or self.mode == 'randrw'):
            fio_cmd += ' --rwmixread=%s --rwmixwrite=%s' % (self.rwmixread, self.rwmixwrite)
        fio_cmd += ' --ioengine=%s' % self.ioengine
        if self.time is not None:
            fio_cmd += ' --runtime=%s' % self.time
        if self.ramp is not None:
            fio_cmd += ' --ramp_time=%s' % self.ramp
        fio_cmd += ' --numjobs=%s' % self.numjobs
        fio_cmd += ' --direct=1'
        fio_cmd += ' --bs=%dB' % self.op_size
        fio_cmd += ' --iodepth=%d' % self.iodepth
        if self.vol_size:
            fio_cmd += ' --size=%dM' % (int(self.vol_size) * 0.9)
        fio_cmd += ' --write_iops_log=%s' % out_file
        fio_cmd += ' --write_bw_log=%s' % out_file
        fio_cmd += ' --write_lat_log=%s' % out_file
        if 'recovery_test' in self.cluster.config:
            fio_cmd += ' --time_based'
        if self.random_distribution is not None:
            fio_cmd += ' --random_distribution=%s' % self.random_distribution
        fio_cmd += ' %s > %s' % (self.names, out_file)
        if self.log_avg_msec is not None:
            fio_cmd += ' --log_avg_msec=%s' % self.log_avg_msec
        print 'Running rbd fio %s test.' % self.mode
        common.pdsh(settings.getnodes('clients'), fio_cmd).communicate()

        # If we were doing recovery, wait until it's done.
        if 'recovery_test' in self.cluster.config:
            self.cluster.wait_recovery_done()

        monitoring.stop(self.run_dir)

        # Finally, get the historic ops
        self.cluster.dump_historic_ops(self.run_dir)
        common.sync_files('%s/*' % self.run_dir, self.out_dir)

    def cleanup(self):
        super(MysqlSysBench, self).cleanup()

    def set_client_param(self, param, value):
        common.pdsh(settings.getnodes('clients'), 'find /sys/block/rbd* -exec sudo sh -c "echo %s > {}/queue/%s" \;' % (value, param)).communicate()

    def __str__(self):
        return "%s\n%s\n%s" % (self.run_dir, self.out_dir, super(MysqlSysBench, self).__str__())

    def mkimages(self):
        monitoring.start("%s/pool_monitoring" % self.run_dir)
        self.cluster.rmpool(self.poolname, self.pool_profile)
        self.cluster.mkpool(self.poolname, self.pool_profile)
        common.pdsh(settings.getnodes('clients'), '/usr/bin/rbd create cbt-mysqlsysbench-`hostname -s` --size %s --pool %s' % (self.vol_size, self.poolname)).communicate()
        common.pdsh(settings.getnodes('clients'), 'sudo rbd map cbt-mysqlsysbench-`hostname -s` --pool %s --id admin' % self.poolname).communicate()
        common.pdsh(settings.getnodes('clients'), 'sudo mkfs.xfs /dev/rbd/cbt-mysqlsysbench/cbt-kernelrbdfio-`hostname -s`').communicate()
        common.pdsh(settings.getnodes('clients'), 'sudo mkdir -p -m0755 -- %s/mnt/cbt-kernelrbdfio-`hostname -s`' % self.cluster.tmp_dir).communicate()
        common.pdsh(settings.getnodes('clients'), 'sudo mount -t xfs -o noatime,inode64 /dev/rbd/cbt-kernelrbdfio/cbt-kernelrbdfio-`hostname -s` %s/mnt/cbt-kernelrbdfio-`hostname -s`' % self.cluster.tmp_dir).communicate()
        monitoring.stop()

    def recovery_callback(self): 
        common.pdsh(settings.getnodes('clients'), 'sudo killall -9 sysbench; sudo killall -9 mysqld').communicate()
