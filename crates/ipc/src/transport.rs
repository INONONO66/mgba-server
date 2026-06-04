//! Async Unix socket transport for the command and frame channels.
//!
//! Two protocols are exposed:
//! - [`IpcServer`] / [`IpcClient`]: length-prefixed JSON for [`crate::WorkerCommand`] and
//!   [`crate::WorkerResponse`] (the control channel).
//! - [`FrameSocketServer`] / [`FrameConnection`]: a packed binary header for the high-frequency
//!   frame data channel.
//!
//! Both protocols use a single connection per socket and assume a 1:1 worker/gateway pairing.

use tokio::io::{AsyncReadExt, AsyncWriteExt};

#[derive(Debug, thiserror::Error)]
pub enum TransportError {
    #[error("IO error: {0}")]
    Io(#[from] std::io::Error),
    #[error("serialization error: {0}")]
    Serialize(#[from] serde_json::Error),
    #[error("connection closed")]
    Closed,
    #[error("frame too large: {0} bytes")]
    FrameTooLarge(u32),
}

const MAX_FRAME_SIZE: u32 = 64 * 1024 * 1024; // 64 MiB

async fn write_framed<W, T>(writer: &mut W, value: &T) -> Result<(), TransportError>
where
    W: AsyncWriteExt + Unpin,
    T: serde::Serialize,
{
    let payload = serde_json::to_vec(value)?;
    let len = payload.len() as u32;
    writer.write_u32(len).await?;
    writer.write_all(&payload).await?;
    Ok(())
}

async fn read_framed<R, T>(reader: &mut R) -> Result<T, TransportError>
where
    R: AsyncReadExt + Unpin,
    T: serde::de::DeserializeOwned,
{
    let len = reader
        .read_u32()
        .await
        .map_err(|_| TransportError::Closed)?;
    if len > MAX_FRAME_SIZE {
        return Err(TransportError::FrameTooLarge(len));
    }
    let mut buf = vec![0u8; len as usize];
    reader
        .read_exact(&mut buf)
        .await
        .map_err(|_| TransportError::Closed)?;
    Ok(serde_json::from_slice(&buf)?)
}

/// Listener side of the command channel.
pub struct IpcServer {
    listener: tokio::net::UnixListener,
}

impl IpcServer {
    /// Bind a Unix domain socket at `path`, removing any stale socket file first.
    pub fn bind(path: &str) -> Result<Self, TransportError> {
        // Remove stale socket file if present; ignore missing.
        let _ = std::fs::remove_file(path);
        let listener = tokio::net::UnixListener::bind(path)?;
        Ok(Self { listener })
    }

    /// Accept a single client connection.
    pub async fn accept(&self) -> Result<IpcConnection, TransportError> {
        let (stream, _addr) = self.listener.accept().await?;
        Ok(IpcConnection::new(stream))
    }
}

/// Server-side connection: receives commands, sends responses.
pub struct IpcConnection {
    reader: tokio::io::ReadHalf<tokio::net::UnixStream>,
    writer: tokio::io::WriteHalf<tokio::net::UnixStream>,
}

impl IpcConnection {
    fn new(stream: tokio::net::UnixStream) -> Self {
        let (reader, writer) = tokio::io::split(stream);
        Self { reader, writer }
    }

    pub async fn recv_command(&mut self) -> Result<crate::WorkerCommand, TransportError> {
        read_framed(&mut self.reader).await
    }

    pub async fn send_response(
        &mut self,
        response: &crate::WorkerResponse,
    ) -> Result<(), TransportError> {
        write_framed(&mut self.writer, response).await
    }
}

/// Client side of the command channel.
pub struct IpcClient {
    reader: tokio::io::ReadHalf<tokio::net::UnixStream>,
    writer: tokio::io::WriteHalf<tokio::net::UnixStream>,
}

impl IpcClient {
    pub async fn connect(path: &str) -> Result<Self, TransportError> {
        let stream = tokio::net::UnixStream::connect(path).await?;
        let (reader, writer) = tokio::io::split(stream);
        Ok(Self { reader, writer })
    }

    pub async fn send_command(
        &mut self,
        command: &crate::WorkerCommand,
    ) -> Result<(), TransportError> {
        write_framed(&mut self.writer, command).await
    }

    pub async fn recv_response(&mut self) -> Result<crate::WorkerResponse, TransportError> {
        read_framed(&mut self.reader).await
    }

    /// Convenience: send a command and wait for the next response.
    pub async fn call(
        &mut self,
        command: crate::WorkerCommand,
    ) -> Result<crate::WorkerResponse, TransportError> {
        self.send_command(&command).await?;
        self.recv_response().await
    }
}

/// Packed frame payload sent across the dedicated frame socket.
///
/// `pixel_format`: `0` = XRGB8888, `1` = RGB565.
#[derive(Debug, Clone)]
pub struct RawFramePacket {
    pub width: u32,
    pub height: u32,
    pub pitch: u32,
    pub pixel_format: u8,
    pub data: Vec<u8>,
}

/// Listener side of the frame channel.
pub struct FrameSocketServer {
    listener: tokio::net::UnixListener,
}

impl FrameSocketServer {
    pub fn bind(path: &str) -> Result<Self, TransportError> {
        let _ = std::fs::remove_file(path);
        let listener = tokio::net::UnixListener::bind(path)?;
        Ok(Self { listener })
    }

    pub async fn accept(&self) -> Result<FrameConnection, TransportError> {
        let (stream, _addr) = self.listener.accept().await?;
        Ok(FrameConnection::new(stream))
    }
}

/// Frame channel connection used by both server and client side.
pub struct FrameConnection {
    reader: tokio::io::ReadHalf<tokio::net::UnixStream>,
    writer: tokio::io::WriteHalf<tokio::net::UnixStream>,
}

impl FrameConnection {
    fn new(stream: tokio::net::UnixStream) -> Self {
        let (reader, writer) = tokio::io::split(stream);
        Self { reader, writer }
    }

    /// Client-side constructor: connect to an existing frame socket.
    pub async fn connect(path: &str) -> Result<Self, TransportError> {
        let stream = tokio::net::UnixStream::connect(path).await?;
        Ok(Self::new(stream))
    }

    pub async fn send_frame(&mut self, frame: &RawFramePacket) -> Result<(), TransportError> {
        self.writer.write_u32(frame.width).await?;
        self.writer.write_u32(frame.height).await?;
        self.writer.write_u32(frame.pitch).await?;
        self.writer.write_u8(frame.pixel_format).await?;
        let data_len = frame.data.len() as u32;
        self.writer.write_u32(data_len).await?;
        self.writer.write_all(&frame.data).await?;
        Ok(())
    }

    pub async fn recv_frame(&mut self) -> Result<RawFramePacket, TransportError> {
        let width = self
            .reader
            .read_u32()
            .await
            .map_err(|_| TransportError::Closed)?;
        let height = self
            .reader
            .read_u32()
            .await
            .map_err(|_| TransportError::Closed)?;
        let pitch = self
            .reader
            .read_u32()
            .await
            .map_err(|_| TransportError::Closed)?;
        let pixel_format = self
            .reader
            .read_u8()
            .await
            .map_err(|_| TransportError::Closed)?;
        let data_len = self
            .reader
            .read_u32()
            .await
            .map_err(|_| TransportError::Closed)?;
        if data_len > MAX_FRAME_SIZE {
            return Err(TransportError::FrameTooLarge(data_len));
        }
        let mut data = vec![0u8; data_len as usize];
        self.reader
            .read_exact(&mut data)
            .await
            .map_err(|_| TransportError::Closed)?;
        Ok(RawFramePacket {
            width,
            height,
            pitch,
            pixel_format,
            data,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{WorkerCommand, WorkerCommandV1, WorkerResponse, WorkerResponseV1};
    use tempfile::tempdir;

    #[tokio::test]
    async fn command_roundtrip() {
        let dir = tempdir().unwrap();
        let socket_path = dir.path().join("test.sock");
        let path_str = socket_path.to_str().unwrap().to_string();

        let server = IpcServer::bind(&path_str).unwrap();

        let client_path = path_str.clone();
        let server_task = tokio::spawn(async move {
            let mut conn = server.accept().await.unwrap();
            let cmd = conn.recv_command().await.unwrap();
            assert!(matches!(cmd, WorkerCommand::V1(WorkerCommandV1::Ping)));
            conn.send_response(&WorkerResponse::V1(WorkerResponseV1::Pong))
                .await
                .unwrap();
        });

        let mut client = IpcClient::connect(&client_path).await.unwrap();
        let response = client
            .call(WorkerCommand::V1(WorkerCommandV1::Ping))
            .await
            .unwrap();
        assert!(matches!(
            response,
            WorkerResponse::V1(WorkerResponseV1::Pong)
        ));

        server_task.await.unwrap();
    }

    #[tokio::test]
    async fn client_handles_server_disconnect() {
        let dir = tempdir().unwrap();
        let socket_path = dir.path().join("test2.sock");
        let path_str = socket_path.to_str().unwrap().to_string();

        let server = IpcServer::bind(&path_str).unwrap();

        let client_path = path_str.clone();
        let server_task = tokio::spawn(async move {
            let _conn = server.accept().await.unwrap();
            // Drop connection immediately
        });

        let mut client = IpcClient::connect(&client_path).await.unwrap();
        server_task.await.unwrap();

        // After server drops, recv should return error
        let result = client.recv_response().await;
        assert!(result.is_err(), "expected error after server disconnect");
    }

    #[tokio::test]
    async fn larger_payload_roundtrip() {
        let dir = tempdir().unwrap();
        let socket_path = dir.path().join("payload.sock");
        let path_str = socket_path.to_str().unwrap().to_string();

        let server = IpcServer::bind(&path_str).unwrap();

        let payload = vec![0xAB_u8; 64 * 1024];
        let payload_for_server = payload.clone();
        let server_task = tokio::spawn(async move {
            let mut conn = server.accept().await.unwrap();
            let cmd = conn.recv_command().await.unwrap();
            match cmd {
                WorkerCommand::V1(WorkerCommandV1::WriteMemory { address, data }) => {
                    assert_eq!(address, 0x0200_0000);
                    assert_eq!(data, payload_for_server);
                }
                other => panic!("unexpected command: {other:?}"),
            }
            conn.send_response(&WorkerResponse::V1(WorkerResponseV1::Ok))
                .await
                .unwrap();
        });

        let mut client = IpcClient::connect(&path_str).await.unwrap();
        let response = client
            .call(WorkerCommand::V1(WorkerCommandV1::WriteMemory {
                address: 0x0200_0000,
                data: payload,
            }))
            .await
            .unwrap();
        assert!(matches!(response, WorkerResponse::V1(WorkerResponseV1::Ok)));

        server_task.await.unwrap();
    }

    #[tokio::test]
    async fn frame_socket_roundtrip() {
        let dir = tempdir().unwrap();
        let socket_path = dir.path().join("frames.sock");
        let path_str = socket_path.to_str().unwrap().to_string();

        let server = FrameSocketServer::bind(&path_str).unwrap();

        let frame = RawFramePacket {
            width: 240,
            height: 160,
            pitch: 960,
            pixel_format: 0,
            data: vec![0x11; 240 * 160 * 4],
        };
        let frame_for_server = frame.clone();

        let server_task = tokio::spawn(async move {
            let mut conn = server.accept().await.unwrap();
            let received = conn.recv_frame().await.unwrap();
            assert_eq!(received.width, frame_for_server.width);
            assert_eq!(received.height, frame_for_server.height);
            assert_eq!(received.pitch, frame_for_server.pitch);
            assert_eq!(received.pixel_format, frame_for_server.pixel_format);
            assert_eq!(received.data, frame_for_server.data);
        });

        let mut client = FrameConnection::connect(&path_str).await.unwrap();
        client.send_frame(&frame).await.unwrap();

        server_task.await.unwrap();
    }
}
