use crate::{prelude::*, types::node_data::EventSource};
use fs::File;
use io::Read;
use std::path::PathBuf;

// We want all of these functions to be synchronous just for ease of use since they are fast (for now)
// Asynchronous stuff can be done in the listen function (waiting for next file event)
pub(crate) trait DirectoryListener {
    // are we tracking a file right now
    fn is_reading(&self, event_source: EventSource) -> bool;
    // get file that we are tracking
    fn file_mut(&mut self, event_source: EventSource) -> &mut Option<File>;
    // when file is created what do we do?
    fn on_file_creation(&mut self, new_file: PathBuf, event_source: EventSource) -> Result<()>;
    // how do we want to process data that we just processed?
    fn process_data(&mut self, data: String, event_source: EventSource) -> Result<()>;

    fn on_file_modification(&mut self, event_source: EventSource) -> Result<()> {
        let mut buf = String::new();
        let file = self.file_mut(event_source).as_mut().ok_or("No file being tracked")?;
        file.read_to_string(&mut buf)?;
        self.process_data(buf, event_source)?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use crate::{
        listeners::directory::{DirectoryListener, EventSource},
        prelude::*,
    };
    use fs::{File, create_dir_all, read_dir, remove_dir_all, remove_file};
    use log::{error, info};
    use notify::{RecursiveMode, Watcher, recommended_watcher};
    use rand::{Rng, SeedableRng, rngs::StdRng};
    use std::{
        io::{Seek, SeekFrom},
        path::{Path, PathBuf},
        sync::{Arc, Mutex},
        time::Duration,
    };
    use tokio::{fs::File as TokioFile, io::AsyncWriteExt, sync::mpsc::unbounded_channel, time::sleep};

    const MOCK_HL_DIR: &str = "tmp/ws_listener_test";
    const DATA: [&str; 2] = [
        r#"{"coin":"@151","side":"A","time":"2025-06-24T02:56:36.172847427","px":"2393.9","sz":"0.1539","hash":"0x2b21750229be769650b604261eaac1018c00c45812652efbbdd35fe0ecb201a1","trade_dir_override":"Na","side_info":[{"user":"0xecb63caa47c7c4e77f60f1ce858cf28dc2b82b00","start_pos":"1166.565307356","oid":105686971733,"twap_id":null,"cloid":"0x1070fff92506b3ab5e5aec135e5a5ddd"},{"user":"0xb65117c1e1006e7b2413fa90e96fcbe3fa83ed75","start_pos":"0.153928559","oid":105686976226,"twap_id":null,"cloid":null}]}
{"coin":"@166","side":"A","time":"2025-06-24T02:56:36.172847427","px":"1.0003","sz":"184.11","hash":"0x0ffc6896b2147680820e04261eaac1018c0101735014e44b56f038478b13ad8f","trade_dir_override":"Na","side_info":[{"user":"0x107332a1729ba0bcf6171117815a87b72a7e6082","start_pos":"36301.55539655","oid":105686050113,"twap_id":null,"cloid":null},{"user":"0xb65117c1e1006e7b2413fa90e96fcbe3fa83ed75","start_pos":"184.12704003","oid":105686976227,"twap_id":null,"cloid":null}]}
{"coin":"@107","side":"B","time":"2025-06-24T02:56:36.172847427","px":"36.796","sz":"15.0","hash":"0x0f0fd1630a15ee0cfbf704261eaac1018c01021cbd299fd0aab0fc4b47dbfeb5","trade_dir_override":"Na","side_info":[{"user":"0xb65117c1e1006e7b2413fa90e96fcbe3fa83ed75","start_pos":"1438.73242692","oid":105686976228,"twap_id":null,"cloid":null},{"user":"0x91853bcefbdc3447768413e6db7d6e0a81c4be56","start_pos":"1230.04231931","oid":105686976111,"twap_id":null,"cloid":"0x0000000000001371000031839dda1c54"}]}
{"coin":"HYPE","side":"B","time":"2025-06-24T02:56:36.241836440","px":"36.773","sz":"0.74","hash":"0xbc8b4f281951cd10af1704261eaac201910053453aba82fd6f64e9ad5cc3eab8","trade_dir_override":"Na","side_info":[{"user":"0xc2d25de009756d3e2ff8a69fd68a02dd7679a8a2","start_pos":"1.25","oid":105686976375,"twap_id":null,"cloid":null},{"user":"0xaf2ac71f62f341e5823d6985492409e92c940447","start_pos":"100.0","oid":105686976012,"twap_id":null,"cloid":"0x000000000000cba600002935c6911dbf"}]}
{"coin":"@142","side":"A","time":"2025-06-24T02:56:36.241836440","px":"104924.0","sz":"0.01094","hash":"0x4e6ee8e4a2376fb83e1104261eaac2019a004ef0e8feaeee0c04557f2cb8faa1","trade_dir_override":"Na","side_info":[{"user":"0xdcac85ecae7148886029c20e661d848a4de99ce2","start_pos":"2.9973766089","oid":105686964903,"twap_id":null,"cloid":null},{"user":"0xefd3ab65915e35105caa462442c9ecc1346728df","start_pos":"1.4552651155","oid":105686976384,"twap_id":null,"cloid":null}]}
{"coin":"@142","side":"A","time":"2025-06-24T02:56:36.241836440","px":"104924.0","sz":"0.00193","hash":"0x4e6ee8e4a2376fb83e1104261eaac2019a004ef0e8feaeee0c04557f2cb8faa1","trade_dir_override":"Na","side_info":[{"user":"0x15f99ee9c49d80851f3dfb9a899da42ec65a1b1b","start_pos":"0.0000145105","oid":105686976114,"twap_id":null,"cloid":null},{"user":"0xefd3ab65915e35105caa462442c9ecc1346728df","start_pos":"1.4443251155","oid":105686976384,"twap_id":null,"cloid":null}]}
{"coin":"@151","side":"A","time":"2025-06-24T02:56:36.241836440","px":"2393.9","sz":"0.2641","hash":"0x95f3d0546bd85531a6d704261eaac2019b0034d53e7526933291402af6adebf7","trade_dir_override":"Na","side_info":[{"user":"0xecb63caa47c7c4e77f60f1ce858cf28dc2b82b00","start_pos":"1166.719207356","oid":105686971733,"twap_id":null,"cloid":"0x1070fff92506b3ab5e5aec135e5a5ddd"},{"user":"0xefd3ab65915e35105caa462442c9ecc1346728df","start_pos":"49.123091455","oid":105686976385,"twap_id":null,"cloid":null}]}
{"coin":"AI16Z","side":"B","time":"2025-06-24T02:56:36.510075144","px":"0.15823","sz":"208.4","hash":"0xb791283313f742315be004261eaac4012a00bb7b07862d44cba4de6813f76602","trade_dir_override":"Na","side_info":[{"user":"0x010461c14e146ac35fe42271bdc1134ee31c703a","start_pos":"-305401.0","oid":105686976469,"twap_id":null,"cloid":null},{"user":"0xb4321b142b2a03ce20fcab2007ff6990b9acba93","start_pos":"18515.4","oid":105686969474,"twap_id":null,"cloid":"0x0000000000000000de5c5df32db69553"}]}
{"coin":"PURR/USDC","side":"B","time":"2025-06-24T02:56:36.578815265","px":"0.20408","sz":"250.0","hash":"0x100515592168666abbbb04261eaac5018b005440d32a1bba306fc3ab0f0a7ff0","trade_dir_override":"Na","side_info":[{"user":"0x162cc7c861ebd0c06b3d72319201150482518185","start_pos":"1107212.2156400001","oid":105686976573,"twap_id":null,"cloid":"0x000000000000000033d1d30c32c2afaa"},{"user":"0xea419262a97e92b6eee39eb3d01138bf2949bb9d","start_pos":"694623.6427","oid":105686975104,"twap_id":null,"cloid":"0x00740ed68330003d00587dc670f10000"}]}
{"coin":"PENDLE","side":"A","time":"2025-06-24T02:56:36.633936095","px":"3.5816","sz":"83.0","hash":"0xd02d3853fdde2296e34e04261eaac6014d00f37036a4ac1812d05fc71c260025","trade_dir_override":"Na","side_info":[{"user":"0x010461c14e146ac35fe42271bdc1134ee31c703a","start_pos":"25471.0","oid":105686972471,"twap_id":null,"cloid":null},{"user":"0x451333eb2f8230cda918db136623da9c26acea0b","start_pos":"-17530.0","oid":105686976632,"twap_id":null,"cloid":"0xaed6d063c14642c4a723b082c96e25c3"}]}
{"coin":"TRX","side":"A","time":"2025-06-24T02:56:36.633936095","px":"0.27225","sz":"1835.0","hash":"0xee7211f542fb48054fd204261eaac60150000a0c8d0ce81ca4e9785d7027e910","trade_dir_override":"Na","side_info":[{"user":"0xecb63caa47c7c4e77f60f1ce858cf28dc2b82b00","start_pos":"-412654.0","oid":105686952032,"twap_id":null,"cloid":"0x6560df107f5fd7092a10871e274ef9e6"},{"user":"0xfc8c156428a8e48cb8d0356db16e59bec4c0ecea","start_pos":"212816.0","oid":105686976635,"twap_id":null,"cloid":"0xae9f6c0100000000c53a025834064b18"}]}
{"coin":"PURR/USDC","side":"A","time":"2025-06-24T02:56:36.633936095","px":"0.20414","sz":"250.0","hash":"0xa35dc0ec4d0f1be7954104261eaac601660036c5486e43d1298fffb58ed3ef26","trade_dir_override":"Na","side_info":[{"user":"0x162cc7c861ebd0c06b3d72319201150482518185","start_pos":"1107462.1781500001","oid":105686976573,"twap_id":null,"cloid":"0x000000000000000033d1d30c32c2afaa"},{"user":"0x2afe745dfd735e24aae8ac39969140dd0511496b","start_pos":"219651.02052","oid":105686976661,"twap_id":null,"cloid":"0x00000000f373000690d9e43c68ee6b51"}]}
{"coin":"ENA","side":"B","time":"2025-06-24T02:56:36.633936095","px":"0.2685","sz":"73.0","hash":"0xe017f0d464bbf330a7fd04261eaac60168007da9578ec00b69d3540c8e9efa0a","trade_dir_override":"Na","side_info":[{"user":"0x31ca8395cf837de08b24da3f660e77761dfb974b","start_pos":"59657.0","oid":105686976663,"twap_id":null,"cloid":null},{"user":"0x6ba889db7f923622d3548f621ecc2054b80c1817","start_pos":"23809.0","oid":105686931473,"twap_id":null,"cloid":"0x17507337595095386710000000000000"}]}
"#,
        r#"{"coin":"STX","side":"B","time":"2025-06-24T02:56:36.894949789","px":"0.63591","sz":"30.6","hash":"0x7306fd0390c93a1cfdc504261eaac9010400e00556a0aef084a2a8161f025a09","trade_dir_override":"Na","side_info":[{"user":"0x31ca8395cf837de08b24da3f660e77761dfb974b","start_pos":"182153.4","oid":105686977410,"twap_id":null,"cloid":null},{"user":"0xee126cd566febeebee9a715812df601e1e69512c","start_pos":"4854.0","oid":105686843698,"twap_id":null,"cloid":"0x9d2c065a6cf37141775d522dcf38855e"}]}
{"coin":"@107","side":"B","time":"2025-06-24T02:56:36.952195987","px":"36.801","sz":"1.56","hash":"0x5376478958639270d25b04261eaaca02011300b6823c88d3cdeeb6d1217e2b13","trade_dir_override":"Na","side_info":[{"user":"0xefd3ab65915e35105caa462442c9ecc1346728df","start_pos":"17674.17669565","oid":105686977611,"twap_id":null,"cloid":null},{"user":"0x8ac0ec32d282d0841f40c4be5a8ecbe0760e59e3","start_pos":"77.32102068","oid":105686977591,"twap_id":null,"cloid":"0x00000000000000001849c172509f24df"}]}
{"coin":"@59","side":"A","time":"2025-06-24T02:56:36.952195987","px":"6.6472","sz":"8.68","hash":"0x0d39f6431f1c3338229c04261eaaca02011500b6669208468264a9aaa88ed3e6","trade_dir_override":"Na","side_info":[{"user":"0xffffffffffffffffffffffffffffffffffffffff","start_pos":"200293.72","oid":105684367994,"twap_id":null,"cloid":null},{"user":"0xefd3ab65915e35105caa462442c9ecc1346728df","start_pos":"856.17828753","oid":105686977613,"twap_id":null,"cloid":null}]}
{"coin":"@166","side":"A","time":"2025-06-24T02:56:36.952195987","px":"1.0003","sz":"165.32","hash":"0xeed8d454cafed26151fd04261eaaca02011600de5df57ee78bab1b091b056b2e","trade_dir_override":"Na","side_info":[{"user":"0x107332a1729ba0bcf6171117815a87b72a7e6082","start_pos":"36485.61568686","oid":105686050113,"twap_id":null,"cloid":null},{"user":"0xefd3ab65915e35105caa462442c9ecc1346728df","start_pos":"215171.4145885","oid":105686977614,"twap_id":null,"cloid":null}]}
{"coin":"@107","side":"B","time":"2025-06-24T02:56:36.952195987","px":"36.801","sz":"4.49","hash":"0x02c80a45cd822365816d04261eaaca02011700f3c0b5703cca97c90aaca4a785","trade_dir_override":"Na","side_info":[{"user":"0xefd3ab65915e35105caa462442c9ecc1346728df","start_pos":"17675.73631346","oid":105686977615,"twap_id":null,"cloid":null},{"user":"0x8ac0ec32d282d0841f40c4be5a8ecbe0760e59e3","start_pos":"75.76103627","oid":105686977591,"twap_id":null,"cloid":"0x00000000000000001849c172509f24df"}]}
{"coin":"@166","side":"A","time":"2025-06-24T02:56:36.952195987","px":"1.0003","sz":"75.59","hash":"0x9c8b059c249d5b4f183c04261eaaca0201180081b2c5cedaa5a8e9c94ef563e5","trade_dir_override":"Na","side_info":[{"user":"0x107332a1729ba0bcf6171117815a87b72a7e6082","start_pos":"36650.89105047","oid":105686050113,"twap_id":null,"cloid":null},{"user":"0xefd3ab65915e35105caa462442c9ecc1346728df","start_pos":"215006.0945885","oid":105686977616,"twap_id":null,"cloid":null}]}
{"coin":"@107","side":"B","time":"2025-06-24T02:56:36.952195987","px":"36.801","sz":"2.05","hash":"0xe3127dd9595b9aaa8d4e04261eaaca020119004ca6f9a8b1ed9c16358a32c78a","trade_dir_override":"Na","side_info":[{"user":"0xefd3ab65915e35105caa462442c9ecc1346728df","start_pos":"17680.22521342","oid":105686977617,"twap_id":null,"cloid":null},{"user":"0x8ac0ec32d282d0841f40c4be5a8ecbe0760e59e3","start_pos":"71.27108116","oid":105686977591,"twap_id":null,"cloid":"0x00000000000000001849c172509f24df"}]}
{"coin":"@107","side":"B","time":"2025-06-24T02:56:36.952195987","px":"36.801","sz":"1.56","hash":"0x4023d33de912351b1c0704261eaaca02011a00b14e8ba255b1fd8dda6aca0f2a","trade_dir_override":"Na","side_info":[{"user":"0xefd3ab65915e35105caa462442c9ecc1346728df","start_pos":"17682.27471118","oid":105686977618,"twap_id":null,"cloid":null},{"user":"0x8ac0ec32d282d0841f40c4be5a8ecbe0760e59e3","start_pos":"69.22110165","oid":105686977591,"twap_id":null,"cloid":"0x00000000000000001849c172509f24df"}]}
{"coin":"@107","side":"B","time":"2025-06-24T02:56:36.952195987","px":"36.801","sz":"5.95","hash":"0xe2bd19ead6bf48a9212604261eaaca02011b000266b78f2f1943e32da417a24d","trade_dir_override":"Na","side_info":[{"user":"0xefd3ab65915e35105caa462442c9ecc1346728df","start_pos":"17683.83432899","oid":105686977619,"twap_id":null,"cloid":null},{"user":"0x8ac0ec32d282d0841f40c4be5a8ecbe0760e59e3","start_pos":"67.66111724","oid":105686977591,"twap_id":null,"cloid":"0x00000000000000001849c172509f24df"}]}
{"coin":"@166","side":"A","time":"2025-06-24T02:56:36.952195987","px":"1.0003","sz":"57.6","hash":"0x7a1100dee236e5a2693604261eaaca02011c00feab4d04df9d016ac3fc5a0bb2","trade_dir_override":"Na","side_info":[{"user":"0x107332a1729ba0bcf6171117815a87b72a7e6082","start_pos":"36726.46064118","oid":105686050113,"twap_id":null,"cloid":null},{"user":"0xefd3ab65915e35105caa462442c9ecc1346728df","start_pos":"214930.5045885","oid":105686977620,"twap_id":null,"cloid":null}]}
{"coin":"@107","side":"A","time":"2025-06-24T02:56:36.952195987","px":"36.791","sz":"13.6","hash":"0x2165d9c82bb7c82b5a5204261eaaca02012500ffd6df104ef2bd49658b724ecd","trade_dir_override":"Na","side_info":[{"user":"0x023a3d058020fb76cca98f01b3c48c8938a22355","start_pos":"7694.15638066","oid":105686977112,"twap_id":null,"cloid":"0x00000000000008029538332006014039"},{"user":"0xa408cf373d04e8c562ea23c2f49acd88968e56ef","start_pos":"7833.93885203","oid":105686977629,"twap_id":null,"cloid":"0x078273e050854e298a3aada86fbc0020"}]}
{"coin":"ETH","side":"A","time":"2025-06-24T02:56:37.026660548","px":"2391.9","sz":"0.0051","hash":"0x0000000000000000000000000000000000000000000000000000000000000000","trade_dir_override":"Na","side_info":[{"user":"0x31ca8395cf837de08b24da3f660e77761dfb974b","start_pos":"14.2048","oid":105686950111,"twap_id":null,"cloid":null},{"user":"0x937791d5ab5040402370f1fc0d9f8f014d80e90e","start_pos":"-2.6603","oid":105686977630,"twap_id":822427,"cloid":null}]}
"#,
    ];

    async fn listen<L: DirectoryListener>(listener: &mut L, event_source: EventSource, dir: &Path) -> Result<()> {
        let event_source_dir = event_source.event_source_dir(dir).canonicalize()?;
        info!("Monitoring directory: {}", event_source_dir.display());
        // monitoring the directory via the notify crate (gives file system events)
        let (fs_event_tx, mut fs_event_rx) = unbounded_channel();
        let mut watcher = recommended_watcher(move |res| {
            let fs_event_tx = fs_event_tx.clone();
            if let Err(err) = fs_event_tx.send(res) {
                error!("Error sending event to processor via channel: {err}");
            }
        })?;

        watcher.watch(&event_source_dir, RecursiveMode::Recursive)?;
        loop {
            match fs_event_rx.recv().await {
                Some(Ok(event)) => {
                    // if a new file is created, start tracking it.
                    if event.kind.is_create() {
                        let new_path = &event.paths[0];
                        if new_path.is_file() {
                            info!("-- Event: {} created --", new_path.display());
                            listener.on_file_creation(new_path.clone(), event_source)?;
                        }
                    }
                    // Check for `Modify` event (only if the file is already initialized)
                    else if event.kind.is_modify() {
                        let new_path = &event.paths[0];
                        if new_path.is_file() {
                            // If we are not tracking anything right now, we treat a file update as declaring that it has been created.
                            // Unfortunately, we miss the update that occurs at this time step.
                            // We go to the end of the file to read for updates after that.
                            if listener.is_reading(event_source) {
                                info!("-- Event: {} modified --", new_path.display());
                                listener.on_file_modification(event_source)?;
                            } else {
                                info!("-- Event: {} created --", new_path.display());
                                let file = listener.file_mut(event_source);
                                let mut new_file = File::open(new_path)?;
                                new_file.seek(SeekFrom::End(0))?;
                                *file = Some(new_file);
                            }
                        }
                    }
                }
                Some(Err(err)) => {
                    error!("Watcher error: {err}");
                    return Err(Box::new(err));
                }
                None => {
                    // The channel disconnected, likely because the sender (watcher) was dropped.
                    // This usually means the program is shutting down or there's a problem.
                    error!("Channel closed. Listener exiting");
                    return Err("Channel closed.".into()); // Exit the loop
                }
            }
        }
    }

    fn clear_dir_contents(path: &Path) -> Result<()> {
        if path.is_dir() {
            for entry in read_dir(path)? {
                let entry = entry?;
                let path = entry.path();
                if path.is_dir() {
                    remove_dir_all(&path)?;
                } else {
                    remove_file(&path)?;
                }
            }
        }
        Ok(())
    }

    async fn create_mock_data(event_source: EventSource, mock_dir: &Path) -> Result<String> {
        // set up so that the directory is initially empty
        let mut res = String::new();
        sleep(Duration::from_millis(100)).await;
        let mut rng = StdRng::from_seed([42; 32]);
        let mock_dir = event_source.event_source_dir(mock_dir).canonicalize()?;
        clear_dir_contents(&mock_dir)?;
        let mock_dir = mock_dir.join("hourly/20250624");
        create_dir_all(&mock_dir)?;

        // test that it works when we transition across files
        for (i, data) in DATA.iter().enumerate() {
            res += data;
            let lines = data.split_whitespace();
            let mut mock_file = TokioFile::create(mock_dir.join((i + 1).to_string())).await?;
            for line in lines {
                mock_file.write_all((line.to_string() + "\n").as_bytes()).await?;
                mock_file.flush().await?;
                let wait = rng.random_bool(0.25);
                // simulate writing transactions in separate blocks by waiting 0.1 seconds.
                if wait {
                    sleep(Duration::from_millis(100)).await;
                }
            }
        }
        Ok(res)
    }

    // will listen to file events and collect their results in the history field
    struct TestListener {
        file: Option<File>,
        history: Arc<Mutex<String>>,
    }

    impl DirectoryListener for TestListener {
        fn is_reading(&self, _event_source: EventSource) -> bool {
            self.file.is_some()
        }

        fn file_mut(&mut self, _event_source: EventSource) -> &mut Option<File> {
            &mut self.file
        }

        fn on_file_creation(&mut self, new_file: PathBuf, _event_source: EventSource) -> Result<()> {
            let file = File::open(new_file)?;
            self.file = Some(file);
            Ok(())
        }

        #[allow(clippy::significant_drop_tightening)]
        fn process_data(&mut self, data: String, _event_source: EventSource) -> Result<()> {
            let mut history = self.history.lock().unwrap();
            *history += &data;
            Ok(())
        }
    }

    impl TestListener {
        fn new(history: Arc<Mutex<String>>) -> Self {
            Self { file: None, history }
        }
    }

    #[allow(clippy::unwrap_used)]
    #[allow(clippy::significant_drop_tightening)]
    #[tokio::test]
    async fn test_trade_listener() -> Result<()> {
        let mock_path = PathBuf::from(MOCK_HL_DIR);
        let event_source = EventSource::Fills;
        create_dir_all(event_source.event_source_dir(&mock_path))?;
        let history = Arc::new(Mutex::new(String::new()));
        let mut test_listener = TestListener::new(history.clone());
        {
            let mock_path = mock_path.clone();
            tokio::spawn(async move {
                if let Err(err) = listen(&mut test_listener, event_source, &mock_path).await {
                    error!("Listener error: {err}");
                }
            });
        }

        // get desired output
        let expected = create_mock_data(event_source, &mock_path).await?;
        sleep(Duration::from_secs(2)).await;
        let history = history.lock().unwrap();
        assert_eq!(*history, expected);
        Ok(())
    }
}
