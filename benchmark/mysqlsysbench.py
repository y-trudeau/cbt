import subprocess
import common
import settings
import monitoring
import os
import time

from benchmark import Benchmark

# this requires Percona sysbench from https://www.percona.com/doc/percona-server/5.5/installation/apt_repo.html 
# or by installing: yum install http://www.percona.com/downloads/percona-release/redhat/0.1-3/percona-release-0.1-3.noarch.rpm 
# on yum based servers
class MysqlSysBench(Benchmark):

    def __init__(self, cluster, config):
        super(MysqlSysBench, self).__init__(cluster, config)

      
        self.cmd_path_full = config.get('cmd_path', '/usr/bin/sysbench')
        self.pool_profile = config.get('pool_profile', 'default')

        #--num-threads=16 --mysql-socket=/var/run/mysqld/mysqld.sock
        #--mysql-database=sbtest --mysql-user=root --test=/usr/share/doc/sysbench/tests/db/oltp.lua --oltp-table-size=50000000
        #  --oltp-test-mode=complex --mysql-engine=innodb --db-driver=mysql --report-interval=60 --max-requests=0 --max-time=3600

        self.threads = config.get('num-threads', 1)
        self.total_procs = self.threads * len(settings.getnodes('clients').split(','))
        
        self.mysql_socket =  str(config.get('mysql-socket', '/tmp/mysqlsysbench.sock'))
        self.mysql_user =  str(config.get('mysql-user', 'sbtest'))
        self.mysql_pass = str(config.get('mysql-password', 'sbtest'))
        self.mysql_database = str(config.get('mysql-database', 'sbtest'))
        self.mysql_engine = str(config.get('mysql-engine', 'innodb'))
        
        self.test_path = str(config.get('test-path', '/usr/share/doc/sysbench/tests/db/oltp.lua'))
        self.prepare_path = str(config.get('prepare-path', '/usr/share/doc/sysbench/tests/db/parallel_prepare.lua'))
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
        self.warmup_time = config.get('warmup-time', 600)
        self.report_time = config.get('report-interval', 60)
        
        self.mycnf_innodb_buffer_pool_size = str(config.get('mycnf-innodb_buffer_pool_size', '128M'))
        self.mycnf_innodb_buffer_pool_instance = config.get('mycnf-innodb_buffer_pool_instance',1)
        self.mycnf_innodb_log_file_size = str(config.get('mycnf-innodb_log_file_size', '128M'))
        self.mycnf_innodb_log_buffer_size = str(config.get('mycnf-innodb_log_buffer_size', '8M'))
        self.mycnf_innodb_read_io_threads = config.get('mycnf-innodb_read_io_threads', 4)
        self.mycnf_innodb_write_io_threads = config.get('mycnf-innodb_write_io_threads', 4)
        self.mycnf_innodb_purge_threads = config.get('mycnf-innodb_purge_threads', 4)
        self.mycnf_innodb_doublewrite = config.get('mycnf-innodb_doublewrite', 1)
        self.mycnf_innodb_file_format = str(config.get('mycnf-innodb_file_format', 'Antelope'))
        self.mycnf_innodb_flush_method = str(config.get('mycnf-innodb_flush_method', 'O_DIRECT'))
        self.mycnf_innodb_flush_log_at_trx_commit = config.get('mycnf-innodb_flush_log_at_trx_commit', 1)
        self.mycnf_innodb_flush_neighbors = config.get('mycnf-innodb_flush_neighbors', 1)

        self.use_local_path = str(config.get('use-local-path', ''))
        if len(self.use_local_path) > 0:
            self.no_rbd = True
            self.mysql_datadir = self.use_local_path
        else:
            self.no_rbd = False
            self.mysql_datadir = '%s/mnt/cbt-mysqlsysbench-`hostname -s`' % self.cluster.tmp_dir
        
        self.use_existing_database = str(config.get('use_existing_database', ''))
        self.existing_database_is_preloaded = 0
        if len(self.use_existing_database) > 0:
            self.no_rbd = True 
            self.no_create_db = True
            self.mysql_database = self.use_existing_database
            self.existing_database_is_preloaded = config.get('existing_database_is_preloaded', 0)
        else:
            self.no_rbd = False
            self.no_create_db = False
        
        self.vol_size = config.get('vol_size', 65536)
        self.vol_order = config.get('vol_order', 22)
        self.vol_format = config.get('vol_format',2)
        self.vol_stripe_unit = config.get('vol_stripe_unit',0)
        self.vol_stripe_count = config.get('vol_stripe_count',0)
        self.random_distribution = config.get('random_distribution', None)
        self.rbdadd_mons = config.get('rbdadd_mons')
        self.rbdadd_options = config.get('rbdadd_options', 'share')
        self.client_ra = config.get('client_ra', 128)
        self.poolname = "cbt-mysqlsysbench"

        #self.run_dir = '%s/mysqlsysbench/ts-%09d' % (self.run_dir, int(time.time()))
        self.out_dir = '%s/ts-%s' % (self.archive_dir, int(time.time()))

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

        if not self.no_rbd:
            print 'Running scrub monitoring.'
            monitoring.start("%s/scrub_monitoring" % self.run_dir)
            self.cluster.check_scrub()
            monitoring.stop()

            print 'Pausing for 60s for idle monitoring.'
            monitoring.start("%s/idle_monitoring" % self.run_dir)
            time.sleep(60)
            monitoring.stop()

        # Create the run directory
        common.make_remote_dir(self.run_dir)
        
        # Create the out directory
        common.make_remote_dir(self.out_dir)

        common.sync_files('%s/*' % self.run_dir, self.out_dir)

        self.mkimages()
 
        if not self.no_create_db:
            # Initialize the datadir
            print 'Running mysql_install_db.'
            mysql_install_cmd = 'sudo /usr/bin/mysql_install_db --no-defaults --datadir=%s --user=mysql --force > %s/mysql_install.out 2> %s/mysqlinstall.err ' % (self.mysql_datadir,self.out_dir,self.out_dir)
            common.pdsh(settings.getnodes('clients'), mysql_install_cmd).communicate()
        
            time.sleep(5)
        
            # Starting MySQL on all nodes
            print 'Starting MySQL'
            mysql_cmd = 'sudo chmod 777 %s; ' % self.out_dir
            mysql_cmd += 'sudo /usr/sbin/mysqld --no-defaults --user=mysql --datadir=%s ' % self.mysql_datadir
            mysql_cmd += '--pid-file=/tmp/mysqlsysbench.pid '
            mysql_cmd += '--innodb-buffer-pool-size=%s ' % self.mycnf_innodb_buffer_pool_size
            mysql_cmd += '--innodb-log-file-size=%s ' % self.mycnf_innodb_log_file_size
            mysql_cmd += '--innodb-log-buffer-size=%s ' % self.mycnf_innodb_log_buffer_size
            mysql_cmd += '--innodb-read-io-threads=%s ' % self.mycnf_innodb_read_io_threads
            mysql_cmd += '--innodb-write-io-threads=%s ' % self.mycnf_innodb_write_io_threads
            mysql_cmd += '--innodb-purge-threads=%s ' % self.mycnf_innodb_purge_threads
            mysql_cmd += '--innodb-doublewrite=%s ' % self.mycnf_innodb_doublewrite
            mysql_cmd += '--innodb-file-format=%s ' % self.mycnf_innodb_file_format
            mysql_cmd += '--innodb-flush-method=%s ' % self.mycnf_innodb_flush_method
            mysql_cmd += '--innodb-flush-log-at-trx-commit=%s ' % self.mycnf_innodb_flush_log_at_trx_commit
            #mysql_cmd += '--innodb-flush-neighbors=%s ' % self.mycnf_innodb_flush_neighbors  # only for percona server
            mysql_cmd += '--log-error=%s/mysqld.log ' % self.out_dir
            mysql_cmd += '--socket=%s ' % self.mysql_socket
            mysql_cmd += '--skip-networking '
            mysql_cmd += '--query-cache-size=0 '
            mysql_cmd += '--innodb-file-per-table ' 
            mysql_cmd += '--skip-performance-schema '
            mysql_cmd += ' > %s/mysql_start.out 2> %s/mysql_start.err ' % (self.out_dir,self.out_dir) 
            mysql_cmd += '&'
            common.pdsh(settings.getnodes('clients'), mysql_cmd).communicate()
        
            #give it time to start up
            print 'Waiting for 60s for mysql to start...'
            time.sleep(60)

            # Create the sysbench tables
            print 'Creating the Sysbench database...'
            mysql_cmd = '/usr/bin/mysql -e "create database sbtest;" '
            mysql_cmd += '-u root '
            mysql_cmd += '--socket=%s ' % self.mysql_socket
            common.pdsh(settings.getnodes('clients'),  mysql_cmd).communicate()
        
        if self.existing_database_is_preloaded = 0:        
            # Creation of the benchmark tables
            print 'Creating the Sysbench tables...'
            pre_cmd = '%s ' % self.cmd_path_full
            pre_cmd += '--test=%s ' % self.prepare_path
            if not self.no_create_db:
                pre_cmd += '--mysql-user=root '
            else:
                pre_cmd += '--mysql-user=%s --mysql-password=%s ' % (self.mysql_user, self.mysql_pass)
            pre_cmd += '--mysql-socket=%s ' % self.mysql_socket
            pre_cmd += '--mysql-db=%s ' % self.mysql_database
            pre_cmd += '--mysql-table-engine=%s ' % self.mysql_engine
            pre_cmd += '--oltp-tables-count=%s ' % self.oltp_table_count
            pre_cmd += '--oltp-table-size=%s ' % self.oltp_table_size
            pre_cmd += '--num-threads=%s run ' % self.threads
            pre_cmd += ' > %s/sysbench_prepare.out 2> %s/sysbench_prepare.err ' % (self.out_dir,self.out_dir)
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

        # Let's warmup MySQL
        time.sleep(5)
        sysbench_cmd = '%s --max-requests=0 ' % (self.cmd_path_full)
        sysbench_cmd += '--max-time=%s ' % self.warmup_time
        sysbench_cmd += '--num-threads=%s ' % self.threads
        sysbench_cmd += '--test=%s ' % self.test_path
        if not self.no_create_db:
            sysbench_cmd += '-u root '
        else:
            sysbench_cmd += '--mysql-user=%s --mysql-password=%s ' % (self.mysql_user, self.mysql_pass)
        sysbench_cmd += '--mysql-db=%s ' % self.mysql_database
        sysbench_cmd += '--mysql-socket=%s ' % self.mysql_socket
        sysbench_cmd += '--oltp-tables-count=%s ' % self.oltp_table_count
        sysbench_cmd += '--oltp-table-size=%s ' % self.oltp_table_size
        sysbench_cmd += '--oltp-read-only=%s ' % self.oltp_read_only
        sysbench_cmd += '--oltp-point-select=%s ' % self.oltp_point_selects
        sysbench_cmd += '--oltp-range-size=%s ' % self.oltp_range_size
        sysbench_cmd += '--oltp-simple-ranges=%s ' % self.oltp_simple_ranges
        sysbench_cmd += '--oltp-sum-ranges=%s ' % self.oltp_sum_ranges
        sysbench_cmd += '--oltp-order-ranges=%s ' % self.oltp_order_ranges
        sysbench_cmd += '--oltp-distinct-ranges=%s ' % self.oltp_distinct_ranges
        sysbench_cmd += '--oltp-index-updates=%s ' % self.oltp_index_updates
        sysbench_cmd += '--oltp-non-index-updates=%s ' % self.oltp_non_index_updates
        sysbench_cmd += 'run '
        sysbench_cmd += ' > %s/sysbench_warmup.out 2> %s/sysbench_warmup.err ' % (self.out_dir,self.out_dir)
        print 'Running sysbench mysql warmup.'
        common.pdsh(settings.getnodes('clients'), sysbench_cmd).communicate()

        # Now the real benchmark
        sysbench_cmd = '%s --max-requests=0 ' % (self.cmd_path_full)
        sysbench_cmd += '--max-time=%s ' % self.max_time
        sysbench_cmd += '--num-threads=%s ' % self.threads
        sysbench_cmd += '--test=%s ' % self.test_path
        if not self.no_create_db:
            sysbench_cmd += '-u root '
        else:
            sysbench_cmd += '--mysql-user=%s --mysql-password=%s ' % (self.mysql_user, self.mysql_pass)
        sysbench_cmd += '--mysql-db=%s ' % self.mysql_database
        sysbench_cmd += '--mysql-socket=%s ' % self.mysql_socket
        sysbench_cmd += '--oltp-tables-count=%s ' % self.oltp_table_count
        sysbench_cmd += '--oltp-table-size=%s ' % self.oltp_table_size
        sysbench_cmd += '--oltp-read-only=%s ' % self.oltp_read_only
        sysbench_cmd += '--oltp-point-select=%s ' % self.oltp_point_selects
        sysbench_cmd += '--oltp-range-size=%s ' % self.oltp_range_size
        sysbench_cmd += '--oltp-simple-ranges=%s ' % self.oltp_simple_ranges
        sysbench_cmd += '--oltp-sum-ranges=%s ' % self.oltp_sum_ranges
        sysbench_cmd += '--oltp-order-ranges=%s ' % self.oltp_order_ranges
        sysbench_cmd += '--oltp-distinct-ranges=%s ' % self.oltp_distinct_ranges
        sysbench_cmd += '--oltp-index-updates=%s ' % self.oltp_index_updates
        sysbench_cmd += '--oltp-non-index-updates=%s ' % self.oltp_non_index_updates
        sysbench_cmd += 'run '
        sysbench_cmd += ' > %s/sysbench.out 2> %s/sysbench.err ' % (self.out_dir,self.out_dir)
        print 'Running sysbench mysql.'
        common.pdsh(settings.getnodes('clients'), sysbench_cmd).communicate()

        monitoring.stop(self.run_dir)

        # Finally, get the historic ops
        self.cluster.dump_historic_ops(self.run_dir)
        common.sync_files('%s/*' % self.run_dir, self.out_dir)

    def cleanup(self):
        super(MysqlSysBench, self).cleanup()
        
        common.pdsh(settings.getnodes('clients'), 'sudo killall -9 sysbench').communicate()
        if not self.no_create_db:
            common.pdsh(settings.getnodes('clients'), 'sudo killall -9 mysqld').communicate()
            common.pdsh(settings.getnodes('clients'), 'sudo rm -rf %s' % self.mysql_datadir).communicate()
        
        if not self.no_rbd:
            common.pdsh(settings.getnodes('clients'), 'sudo umount /dev/rbd/%s/cbt-mysqlsysbench-`hostname -s`' % self.poolname).communicate()
            common.pdsh(settings.getnodes('clients'), 'sudo rbd unmap /dev/rbd/%s/cbt-mysqlsysbench-`hostname -s`' % self.poolname).communicate()
            common.pdsh(settings.getnodes('clients'), 'sudo rbd rm cbt-mysqlsysbench-`hostname -s` --pool %s' %  self.poolname).communicate()
            self.cluster.rmpool(self.poolname, self.pool_profile)
        
    def set_client_param(self, param, value):
        if not self.no_rbd:
            common.pdsh(settings.getnodes('clients'), 'find /sys/block/rbd* -exec sudo sh -c "echo %s > {}/queue/%s" \;' % (value, param)).communicate()

    def __str__(self):
        return "%s\n%s\n%s" % (self.run_dir, self.out_dir, super(MysqlSysBench, self).__str__())

    def mkimages(self):
        
        common.pdsh(settings.getnodes('clients'), 'sudo mkdir -p -m0755 -- %s' % self.mysql_datadir).communicate()
        
        if not self.no_rbd:
            monitoring.start("%s/pool_monitoring" % self.run_dir)
            self.cluster.rmpool(self.poolname, self.pool_profile)
            self.cluster.mkpool(self.poolname, self.pool_profile)
            
            
            rbd_create_cmd = 'sudo rbd create cbt-mysqlsysbench-`hostname -s` --size %s --pool %s ' % (self.vol_size, self.poolname)
            rbd_create_cmd += ' --image-format %s ' %  self.vol_format
            rbd_create_cmd += ' --order %s ' % self.vol_order
            if self.vol_stripe_unit > 0 and self.vol_stripe_count:
                rbd_create_cmd += ' --stripe-unit %s --stripe-count %s ' % (self.vol_stripe_unit,self.vol_stripe_count)
                
            common.pdsh(settings.getnodes('clients'), rbd_create_cmd).communicate()
            common.pdsh(settings.getnodes('clients'), 'sudo rbd map cbt-mysqlsysbench-`hostname -s` --pool %s' % self.poolname).communicate()
            common.pdsh(settings.getnodes('clients'), 'sudo mkfs.xfs /dev/rbd/%s/cbt-mysqlsysbench-`hostname -s`' % self.poolname).communicate()
            common.pdsh(settings.getnodes('clients'), 'sudo mount -t xfs -o rw,noatime,inode64 /dev/rbd/%s/cbt-mysqlsysbench-`hostname -s` %s' % (self.poolname, self.mysql_datadir)).communicate()
            monitoring.stop()

        if not self.no_create_db:
            common.pdsh(settings.getnodes('clients'), 'sudo chown mysql.mysql %s' % self.mysql_datadir).communicate()
        

    def recovery_callback(self): 
        if not self.no_create_db:
            common.pdsh(settings.getnodes('clients'), 'sudo killall -9 mysqld').communicate()

        common.pdsh(settings.getnodes('clients'), 'sudo killall -9 sysbench').communicate()
