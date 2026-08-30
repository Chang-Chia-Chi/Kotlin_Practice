BEGIN { FS="," }
NR==1 { next }
$4 != "200" { next }
{
  n++
  ts[n]=$1; el[n]=$2; rss[n]=$5; fds[n]=$6; meters[n]=$7
  wipgen[n]=$8; wipwal[n]=$9; equipgen[n]=$10; equipwal[n]=$11; scratch[n]=$12
}
END {
  third = int(n/3)
  for (i=1; i<=third; i++) {
    f_rss+=rss[i]; f_fds+=fds[i]; f_met+=meters[i]; f_wal+=wipwal[i]; f_scr+=scratch[i]
  }
  for (i=n-third+1; i<=n; i++) {
    l_rss+=rss[i]; l_fds+=fds[i]; l_met+=meters[i]; l_wal+=wipwal[i]; l_scr+=scratch[i]
  }
  printf "rows=%d third=%d elapsed_first=%s..%ss elapsed_last=%ss..%ss\n", n, third, el[1], el[third], el[n-third+1], el[n]
  printf "RSS_kb        first3rd_avg=%.0f last3rd_avg=%.0f delta=%.0f (%.1f%%)\n", f_rss/third, l_rss/third, (l_rss-f_rss)/third, 100*((l_rss/third)-(f_rss/third))/(f_rss/third)
  printf "open_fds      first3rd_avg=%.1f last3rd_avg=%.1f delta=%.1f\n", f_fds/third, l_fds/third, (l_fds-f_fds)/third
  printf "meter_count   first3rd_avg=%.1f last3rd_avg=%.1f delta=%.1f\n", f_met/third, l_met/third, (l_met-f_met)/third
  printf "wip_wal_bytes first3rd_avg=%.0f last3rd_avg=%.0f delta=%.0f\n", f_wal/third, l_wal/third, (l_wal-f_wal)/third
  printf "scratch_dirs  first3rd_avg=%.2f last3rd_avg=%.2f delta=%.2f\n", f_scr/third, l_scr/third, (l_scr-f_scr)/third
  printf "RSS_kb min=%d max=%d\n", minv(rss,n), maxv(rss,n)
  printf "open_fds min=%d max=%d\n", minv(fds,n), maxv(fds,n)
}
function minv(a,n,  i,m){m=a[1]; for(i=2;i<=n;i++) if(a[i]<m) m=a[i]; return m}
function maxv(a,n,  i,m){m=a[1]; for(i=2;i<=n;i++) if(a[i]>m) m=a[i]; return m}
